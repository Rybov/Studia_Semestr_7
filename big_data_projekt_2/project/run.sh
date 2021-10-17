#!/usr/bin/bash

spark-shell -i create_tables.scala
spark-submit --class W_weather --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 w_weather.jar $USER
spark-submit --class W_place --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 w_place.jar $USER
spark-submit --class W_time --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 w_time.jar $USER
spark-submit --class W_sun --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 w_sun.jar $USER
spark-submit --class F_facts --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 f_facts.jar $USER
