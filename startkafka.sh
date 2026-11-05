#!/bin/bash
cd "$HOME/kafka" || exit 1

bin/kafka-server-start.sh config/server.properties
