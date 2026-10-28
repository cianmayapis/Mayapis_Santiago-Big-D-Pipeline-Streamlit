#!/bin/bash
cd "$HOME/spotify-pipeline" || exit 1

# Uncomment
source ~/miniconda3/etc/profile.d/conda.sh
conda activate mongo

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
spark-submit --packages $PACKAGES consumer.py
# wait until batch records 0
