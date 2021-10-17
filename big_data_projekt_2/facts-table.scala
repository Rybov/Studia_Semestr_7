 spark.sql("""CREATE TABLE `f_facts` (
`id` int,
`accidents_per_hour` float,
`sum_distance` float,
`sum_severity` int,
`is_intersection` boolean,
`is_traffic_signal` boolean,
`hll_accidents_count` float,
`temperature` float,
`place_id` int,
`time_id` int,
`sun_id` int,
`weather_id` int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")