# 🚀 Real-Time & Historical Streaming Data Dashboard

A distributed big data streaming pipeline that ingests, buffers, persists, and visualizes real-time environmental telemetry across 8 monitored cities. The application separates live streaming telemetry from historical analytical reporting using a dual-pipeline architecture built on **Apache Kafka**, **MongoDB Atlas**, and **Streamlit**.

## 🎯 Architecture & Components

The system implements a decoupled dual-pipeline architecture to balance low-latency streaming updates with persistent, non-blocking analytical queries:

```text
                  [ External Weather REST API ]
                                │
                                ▼
                       [ Python Producer ]
                      (environment/producer.py)
                                │
                                ▼ (JSON Events)
                     [ Apache Kafka Broker ]
                        (Port: 9092)
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼ (Live Consumer)                               ▼ (Persistence Sink)
[ Real-Time Stream Engine ]                    [ MongoDB Atlas Cluster ]
(consume_kafka_data())                         (Remote Cloud Database)
        │                                               │
        │                                               ▼ (Point-in-Time Query)
        │                                    [ Historical Query Engine ]
        │                                    (query_historical_data())
        └───────────────────────┬───────────────────────┘
                                ▼
                     [ Streamlit Dashboard ]
                      (environment/app.py)
                 (VM: 8501 ──► Windows Host: 8888)
```

### Pipeline Breakdown

| Pipeline | Flow | Output / View |
| :--- | :--- | :--- |
| **Real-time** | Kafka $\rightarrow$ Streamlit (`consume_kafka_data()`) | `📈 Real-time Streaming View`: Instant live telemetry cards, dynamic sensor gauges, and low-latency sliding charts. |
| **Historical** | Kafka $\rightarrow$ MongoDB Atlas $\rightarrow$ Streamlit (`query_historical_data()`) | `📊 Historical Data View`: Filterable aggregation tables, multi-city metric comparisons, and time-range trends. |

### Mandatory Components Implemented

* **Kafka Producer/Consumer:** Built with `kafka-python-ng` for high-throughput, non-blocking message serialization and consumption.
* **MongoDB Atlas Integration:** Cloud-based persistence layer storing structured JSON records with indexed lookup fields.
* **Two-Page Streamlit Dashboard:** Interactive tabs separating live streaming buffers from historical database queries.
* **Robust Error Handling:** Automated socket reconnection, broker availability checks, and non-blocking consumer timeouts.

## 💻 Technical Implementation Tasks

### 1. Data Producer (`producer.py`)

The producer continuously requests real-time atmospheric and environmental data from an external REST API across 8 monitored locations. Payloads are formatted to comply with the project schema and published to the Kafka topic.

**Enforced Data Schema Fields:**

* `timestamp` (ISO format): Current observation timestamp formatted as an ISO-8601 string (`YYYY-MM-DDTHH:MM:SSZ`).
* `value` (Numeric): Reading of the observed measurement (e.g., Temperature in °C, Humidity in %, Wind Speed in m/s).
* `metric_type` (String): Identifier indicating the measured parameter (e.g., `temperature`, `humidity`, `wind_speed`).
* `sensor_id` (String): Identifier indicating the reporting station or city (e.g., `sensor_city_01`, `sensor_manila`).

### 2. Dashboard (`app.py`)

The Streamlit user interface is organized into a two-page navigation layout:

* **`consume_kafka_data()` (Real-time View):** Consumes streaming events from the Kafka topic using `kafka-python-ng` without blocking UI interactivity. Displays immediate single-metric gauges, operational status badges, and rolling streaming line graphs.
* **`query_historical_data()` (Historical View):** Uses `pymongo` to query MongoDB Atlas. Integrates interactive sidebar widgets including city/sensor dropdown filters, metric pickers, and date-time range sliders for deep-dive analysis.

### 3. Storage Integration (MongoDB Atlas)

* Serves as the persistent cloud sink, decoupling live visualization consumption from Kafka topic retention windows.
* Automatically stores incoming records with indexed `timestamp` and `sensor_id` fields for query retrieval and aggregation.

## 🏃‍♂️ Setup & Execution

### Prerequisites

* Ubuntu Linux environment (Virtual Machine or Bare Metal) with OpenSSH Server.
* Windows Host with OpenSSH client and Visual Studio Code (Remote - SSH).
* Miniconda / Anaconda with Python 3.10.13.
* Running Apache Kafka broker on `localhost:9092`.
* MongoDB Atlas cluster with network access whitelisted for the VM's public IP.

### Step 1: Environment Setup

1. **Create and activate the Conda environment:**

```bash
conda create -n bigdata python=3.10.13 -y
conda activate bigdata
```

2. **Clone the repository & install dependencies:**

```bash
git clone [https://github.com/cianmayapis/Mayapis_Santiago-Big-D-Pipeline-Streamlit.git](https://github.com/cianmayapis/Mayapis_Santiago-Big-D-Pipeline-Streamlit.git)
cd Mayapis_Santiago-Big-D-Pipeline-Streamlit
pip install -r requirements.txt
```

3. **Environment Configuration (`.env`):**
   Create a `.env` file inside `environment/` to store your connection credentials:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=weather_data
MONGO_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
MONGO_DB_NAME=streaming_pipeline
MONGO_COLLECTION=sensor_metrics
```

### Step 2: Execution Workflow (Ubuntu VM via VS Code)

Open three dedicated terminal sessions in Visual Studio Code connected to the Ubuntu VM via SSH:

#### Terminal 1: Start Kafka Broker & Services

Clean up any lingering processes on port `8501` and launch Kafka:

```bash
sudo fuser -k 8501/tcp
bash services.sh start
```

*Verification:* Run `ss -tulpn | grep 9092` to confirm the Kafka broker is listening.

#### Terminal 2: Start Data Producer

Start ingestion from the API into the Kafka topic:

```bash
conda activate bigdata
cd ~/Mayapis_Santiago-Big-D-Pipeline-Streamlit/environment
python producer.py --interval 5
```

*Verification:* The console prints JSON ingestion records for each of the 8 cities cyclically every 5 seconds.

#### Terminal 3: Launch Streamlit Dashboard

Start the web dashboard bound to all interfaces on port `8501`:

```bash
conda activate bigdata
cd ~/Mayapis_Santiago-Big-D-Pipeline-Streamlit/environment
python -m streamlit run app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

*Verification:* The terminal logs `Uvicorn server started on 0.0.0.0:8501`.

### Step 3: Accessing the Dashboard (Windows Host)

Because the VM operates behind NAT virtualization, route traffic through a direct IPv4 SSH tunnel to prevent socket collisions:

1. Open **Command Prompt (`cmd.exe`)** or **PowerShell** on your **Windows host**:

```cmd
ssh -N -L 8888:127.0.0.1:8501 -p 2222 santiago@127.0.0.1
```

2. Enter your Ubuntu VM password when prompted. Keep this terminal window open in the background.
3. Open your Windows web browser and navigate to:

```text
[http://127.0.0.1:8888](http://127.0.0.1:8888)
```

## 📦 Deliverables & Repository Structure

```text
Mayapis_Santiago-Big-D-Pipeline-Streamlit/
│
├── environment/
│   ├── app.py              # Streamlit dashboard implementation (Real-time & Historical views)
│   ├── producer.py         # Kafka producer querying REST API and publishing JSON events
│   └── .env                # Connection strings for Kafka and MongoDB Atlas (gitignored)
│
├── services.sh             # Automation script to start Kafka services
├── requirements.txt        # Pinned Python package dependencies
└── README.md               # Project architecture, documentation, and setup guide
```

## 🛠️ Key Dependencies (`requirements.txt`)

```text
streamlit>=1.30.0
kafka-python-ng>=2.2.0
pymongo>=4.6.0
dnspython>=2.4.0
pandas>=2.1.0
requests>=2.31.0
python-dotenv>=1.0.0
altair>=5.2.0
```
