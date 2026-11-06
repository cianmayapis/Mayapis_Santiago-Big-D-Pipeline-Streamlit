"""
Kafka Producer for Streaming Data Dashboard
Data source: WeatherAPI.com Realtime API (current.json)

Each API call returns the current conditions for ONE city. The producer cycles
through a list of cities (round-robin) and publishes up to three messages per call
(temperature, humidity, pressure) using the dashboard's required schema:

    {"timestamp": "2026-11-06T08:15:00Z", "value": 27.4, "metric_type": "temperature",
     "sensor_id": "manila_philippines", "location": "Manila, Philippines", "unit": "celsius"}

Design notes
- WeatherAPI refreshes realtime data every 10-15 minutes, so a reading is only
  published when `last_updated_epoch` has changed for that city (no duplicates).
- The free plan allows 100,000 calls/month; the poll interval is a budget decision.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv()  # reads WEATHERAPI_KEY (and optional overrides) from .env

API_URL = "https://api.weatherapi.com/v1/current.json"
FREE_PLAN_MONTHLY_CALLS = 100_000

DEFAULT_CITIES = [
    "Manila", "Cebu", "Davao", "Baguio",
    "Tokyo", "Singapore", "Sydney", "London",
]

# (metric_type, field in the API's "current" object, unit)
METRICS = [
    ("temperature", "temp_c", "celsius"),
    ("humidity", "humidity", "percent"),
    ("pressure", "pressure_mb", "hPa"),  # 1 millibar == 1 hPa
]


class FatalAPIError(Exception):
    """Invalid key, disabled key, or quota exceeded: retrying will not help."""


class StreamingDataProducer:
    """
    Kafka producer that turns WeatherAPI.com realtime readings into messages.
    Handles the Kafka connection, API polling, deduplication and message sending.
    """

    def __init__(self, bootstrap_servers: str, topic: str,
                 api_key: Optional[str] = None, cities: Optional[List[str]] = None):
        """
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        - api_key: WeatherAPI.com key (defaults to the WEATHERAPI_KEY env var)
        - cities: location queries for the API's `q` parameter (city names or "lat,lon")
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key or os.getenv("WEATHERAPI_KEY")
        self.cities = cities or DEFAULT_CITIES

        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'acks': 'all',      # wait for the broker to confirm each write
            'retries': 3,
            'linger_ms': 10,
        }

        # API polling state
        self.session = requests.Session()
        self.city_index = 0          # round-robin pointer
        self.api_calls = 0           # calls made this run (quota awareness)
        self.last_epoch: Dict[str, int] = {}  # sensor_id -> last published last_updated_epoch

        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    # ------------------------------------------------------------------ API
    @staticmethod
    def _slug(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")

    @staticmethod
    def _api_error_message(resp: requests.Response) -> str:
        try:
            err = resp.json().get("error", {})
            return f"HTTP {resp.status_code}, code {err.get('code')}: {err.get('message')}"
        except ValueError:
            return f"HTTP {resp.status_code}"

    def fetch_current(self, query: str) -> Optional[Dict[str, Any]]:
        """Call current.json for one location. Returns parsed JSON or None to skip."""
        self.api_calls += 1
        try:
            resp = self.session.get(
                API_URL,
                params={"key": self.api_key, "q": query, "aqi": "no"},
                timeout=10,
            )
        except requests.RequestException as e:
            print(f"WARN: network error while fetching '{query}': {e}")
            return None

        if resp.status_code in (401, 403):
            # Bad/disabled key or monthly quota exhausted
            raise FatalAPIError(self._api_error_message(resp))
        if resp.status_code != 200:
            print(f"WARN: API error for '{query}': {self._api_error_message(resp)}")
            return None
        try:
            return resp.json()
        except ValueError:
            print(f"WARN: API returned invalid JSON for '{query}'")
            return None

    def generate_sample_data(self) -> List[Dict[str, Any]]:
        """
        Fetch REAL data for the next city in the rotation and convert it into
        dashboard-compatible records (one per metric).

        Returns a list because one API call yields several metrics. The list is
        empty when the API has not published a newer reading for that city.
        """
        query = self.cities[self.city_index % len(self.cities)]
        self.city_index += 1

        payload = self.fetch_current(query)
        if not payload:
            return []

        loc = payload.get("location", {})
        cur = payload.get("current", {})
        epoch = cur.get("last_updated_epoch")
        if epoch is None:
            print(f"WARN: no last_updated_epoch in response for '{query}'")
            return []

        sensor_id = self._slug(f"{loc.get('name', query)}_{loc.get('country', '')}")

        # Skip duplicates: the API only refreshes every ~10-15 minutes
        if self.last_epoch.get(sensor_id) == epoch:
            return []
        self.last_epoch[sensor_id] = epoch

        timestamp = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        location = ", ".join(p for p in (loc.get("name"), loc.get("country")) if p) or query

        records = []
        for metric_type, field, unit in METRICS:
            value = cur.get(field)
            if value is None:
                continue
            records.append({
                "timestamp": timestamp,
                "value": float(value),
                "metric_type": metric_type,
                "sensor_id": sensor_id,
                "location": location,
                "unit": unit,
            })
        return records

    # ---------------------------------------------------------------- Kafka
    def serialize_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        """Serialize the record as UTF-8 JSON bytes (None on failure)."""
        try:
            return json.dumps(data).encode('utf-8')
        except (TypeError, ValueError) as e:
            print(f"ERROR: could not serialize {data}: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Send one record to Kafka. The sensor_id is used as the message key so all
        readings of a city stay ordered within a partition.

        Returns True if the broker confirmed the message, False otherwise.
        """
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False

        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False

        try:
            future = self.producer.send(
                self.topic,
                key=data["sensor_id"].encode('utf-8'),
                value=serialized_data,
            )
            future.get(timeout=10)  # wait for confirmation
            print(f"Sent: {data['sensor_id']} {data['metric_type']}={data['value']} {data['unit']} @ {data['timestamp']}")
            return True
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 1 / 30, duration: int = None):
        """
        Main streaming loop.

        Parameters:
        - messages_per_second: here, API POLLS per second (default 1/30 = one API call
          every 30 s). Each poll can produce up to 3 messages.
        - duration: Total runtime in seconds (None for infinite)
        """
        if not self.producer:
            print("ERROR: no Kafka connection; nothing to do.")
            return

        interval = 1.0 / messages_per_second
        projected = 30 * 24 * 3600 / interval
        print(f"Starting producer: one API call every {interval:.1f}s across {len(self.cities)} cities "
              f"({len(self.cities) and interval * len(self.cities):.0f}s per city), "
              f"duration: {duration or 'infinite'}")
        if projected > FREE_PLAN_MONTHLY_CALLS:
            print(f"WARN: running nonstop at this rate is ~{projected:,.0f} calls/month, "
                  f"above the free plan's {FREE_PLAN_MONTHLY_CALLS:,}.")

        start_time = time.time()
        message_count = 0

        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break

                cycle_start = time.time()
                records = self.generate_sample_data()
                if not records:
                    print(f"No new reading this poll (API calls so far: {self.api_calls})")
                for data in records:
                    if self.send_message(data):
                        message_count += 1

                # Sleep the remainder of the interval
                time.sleep(max(0.0, interval - (time.time() - cycle_start)))

        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except FatalAPIError as e:
            print(f"FATAL: WeatherAPI rejected the request ({e}). Check WEATHERAPI_KEY / quota.")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(f"Producer stopped. Messages sent: {message_count}, API calls: {self.api_calls}")

    def close(self):
        """Flush pending messages and release resources."""
        self.session.close()
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """Command-line arguments (defaults can also come from environment variables)."""
    parser = argparse.ArgumentParser(description='WeatherAPI -> Kafka Streaming Producer')

    parser.add_argument('--bootstrap-servers', type=str,
                        default=os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', type=str,
                        default=os.getenv('KAFKA_TOPIC', 'streaming-data'),
                        help='Kafka topic to produce to (default: streaming-data)')
    parser.add_argument('--rate', type=float, default=1 / 30,
                        help='API polls per second (default: 1/30, one call every 30 s)')
    parser.add_argument('--interval', type=float, default=None,
                        help='Seconds between API calls (overrides --rate)')
    parser.add_argument('--duration', type=int, default=None,
                        help='Run duration in seconds (default: infinite)')
    parser.add_argument('--cities', type=str, default=None,
                        help='Comma-separated locations, e.g. "Manila,Tokyo,14.6,121.0"')

    return parser.parse_args()


def main():
    print("=" * 60)
    print("WEATHERAPI -> KAFKA PRODUCER")
    print("=" * 60)

    args = parse_arguments()

    api_key = os.getenv("WEATHERAPI_KEY")
    if not api_key:
        print("ERROR: WEATHERAPI_KEY is not set. Add it to a .env file (see .env.example).")
        sys.exit(1)

    cities = [c.strip() for c in args.cities.split(",") if c.strip()] if args.cities else None
    rate = 1.0 / args.interval if args.interval else args.rate
    if rate <= 0:
        print("ERROR: rate/interval must be positive.")
        sys.exit(1)

    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        api_key=api_key,
        cities=cities,
    )
    if not producer.producer:
        sys.exit(1)

    try:
        producer.produce_stream(messages_per_second=rate, duration=args.duration)
    except Exception as e:
        print(f"Main execution error: {e}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()