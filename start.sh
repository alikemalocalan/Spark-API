#!/usr/bin/env bash
spark-submit --master local[*] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 server.py