#!/bin/bash
bash startkafka.sh 
sleep 10

bash startconsumer.sh
sleep 10

bash startproducer.sh