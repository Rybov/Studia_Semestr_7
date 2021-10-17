spark.sql("show databases").show()
spark.sql("""DROP TABLE IF EXISTS `w_place`""")
spark.sql("""DROP TABLE IF EXISTS `w_time`""")
spark.sql("""DROP TABLE IF EXISTS `w_weather`""")
spark.sql("""DROP TABLE IF EXISTS `w_sun`""")
spark.sql("""DROP TABLE IF EXISTS `f_facts`""")

spark.sql("""CREATE TABLE `w_place` (
 `Street` string,
 `City` string,
 `County` string,
 `State` string,
 `Zipcode` string,
 `id` long)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_time` (
 `hour` int,
 `day` int,
 `month` int,
 `quarter` int,
 `year` int,
 `id` long)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_sun` (
 `Sunrise_Sunset` string,
 `Civil_Twilight` string,
 `id` long)
 ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_weather` (
  `time` timestamp,
  `airport` string,
  `temperature`double,
  `humidity`double,
  `weather_condition` string,
  `id` long)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


spark.sql("""CREATE TABLE `f_facts` (
 `is_intersection` boolean,
 `is_traffic_signal` boolean,
 `time_id` long,
 `place_id` long,
 `sun_id` long,
 `weather_id` long,
 `accidents_per_three_hour` double,
 `sum_severity` double,
 `sum_distance` double,
 `sum_parts_accidents` double,
 `id` long)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

System.exit(0)
