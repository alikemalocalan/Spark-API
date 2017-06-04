#!/usr/bin/env bash
mkdir /tmp/spark-events/
spark-submit  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 server.py
