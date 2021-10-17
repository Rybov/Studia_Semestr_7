// Import danych z csv do sparka
val mainDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/mainDataCentral.csv").cache();
val mainDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/mainDataEastern.csv").cache();
val mainDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/mainDataMountain.csv").cache();
val mainDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/mainDataPacific.csv").cache();
val geoDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/geoDataCentral.csv").cache();
val geoDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/geoDataEastern.csv").cache();
val geoDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/geoDataMountain.csv").cache();
val geoDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/jankowalczyk38/us-accidents/geoDataPacific.csv").cache();
val weatherToParse = spark.read.text(s"/user/jankowalczyk38/us-accidents/weather.txt")



import java.text.SimpleDateFormat
import java.sql.Timestamp
 
val mainData = mainDataMountain
// val mainData = mainDataCentral
//   .union(mainDataEastern)
//   .union(mainDataMountain)
//   .union(mainDataPacific)
//   .dropDuplicates(Array("ID"))
  
val geoData = geoDataCentral
  .union(geoDataEastern)
  .union(geoDataMountain)
  .union(geoDataPacific)
  .dropDuplicates(Array("Zipcode"))

def getDateTime(dateTime: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy-dd-MM' 'HH:mm:ss")
    val x = format.parse(dateTime.dropRight(2))
    return new Timestamp(x.getTime())
}

def getCondition(line:String):String = {
  return line.split("Condition: ")(1)
}

val weather = weatherToParse.map(line => {
      val data = line.getString(0).split(" ")
      ( getDateTime(data(1) + " " + data(2)),
        data(10),
        data(19).dropRight(1), 
        data(26).dropRight(1),
        getCondition(line.getString(0))
        )
    })
    .withColumnRenamed("_1", "time")
    .withColumnRenamed("_2", "airport")
    .withColumnRenamed("_3", "temperature")
    .withColumnRenamed("_4", "humidity")
    .withColumnRenamed("_5", "conditition")
    .filter(($"temperature" !== "~") && ($"humidity" !== "~"))


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
`id` int,
`accidents_per_hour` float,
`sum_distance` float,
`sum_severity` int,
`is_intersection` boolean,
`is_traffic_signal` boolean,
`hll_accidents_count` float,
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
 

// TIMEDATA 
import org.apache.spark.sql._
import spark.implicits._
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer


def getStartDate(startDate:Timestamp) :LocalDateTime={
    val date = startDate.toLocalDateTime().withMinute(0).withSecond(0)
    val hour = date.getHour()
    if(hour>=21)
        return date.withHour(21)
    else if(hour>=18)
        return date.withHour(18)
    else if(hour>=15)
        return date.withHour(15)
    else if(hour>=12)
        return date.withHour(12)
    else if(hour>=9)
        return date.withHour(9)
    else if(hour>=6)
        return date.withHour(6)
    else if(hour>=3)
        return date.withHour(3)
    else (hour>=0)
        return date.withHour(0)
}

def getTimeData(startTime:Timestamp,endTime:Timestamp):List[Timestamp] ={
    var sDate = getStartDate(startTime)
    val eDate = endTime.toLocalDateTime()
    var dates = new ListBuffer[Timestamp]()
    while(eDate.isAfter(sDate)){
        dates += Timestamp.valueOf(sDate)
        sDate = sDate.plusHours(3)
    }
    return dates.toList
}



val accdata = mainData.flatMap(row => {
    getTimeData(row.get(4).asInstanceOf[Timestamp],row.get(5).asInstanceOf[Timestamp])
    .map(x => (
    row.get(0).asInstanceOf[String],
    row.get(3).asInstanceOf[Int],
    row.get(10).asInstanceOf[Double],
    row.get(13).asInstanceOf[String],
    row.get(15).asInstanceOf[String],
    row.get(16).asInstanceOf[String],
    row.get(19).asInstanceOf[Boolean] || row.get(24).asInstanceOf[Boolean],
    row.get(28).asInstanceOf[Boolean],
    row.get(30).asInstanceOf[String],
    row.get(31).asInstanceOf[String],
    x))
}).withColumnRenamed("_1", "ID")
.withColumnRenamed("_2", "Severity")
.withColumnRenamed("_3", "Distance")
.withColumnRenamed("_4", "Street")
.withColumnRenamed("_5", "ZIPCODE")
.withColumnRenamed("_6", "AirPort")
.withColumnRenamed("_7", "Is_Intersection")
.withColumnRenamed("_8", "Signal")
.withColumnRenamed("_9", "Sunrise_Sunset")
.withColumnRenamed("_10", "Civil_Twilight")
.withColumnRenamed("_11", "Time")

accdata.count()

// W_PLACE
val w_placeData = geoData.join(mainData, mainData("ZipCode") === geoData("ZipCode"))
  .select(mainData("Street"), geoData("City"), geoData("County"), geoData("State"),geoData("Zipcode"))
  .distinct()
  .withColumn("id", monotonically_increasing_id+1)

//w_SUN
val w_sunData = accdata.select($"Sunrise_Sunset", $"Civil_Twilight")
.distinct()
.withColumn("id", monotonically_increasing_id+1)

//W_WEATHER
val w_weatherData = weather.distinct().withColumn("id", monotonically_increasing_id+1)

//W_TIME
val w_timeData = accdata.select(
    hour($"Time").alias("hour"),
    dayofmonth($"Time").alias("day"),
    month($"Time").alias("month"),
    quarter ($"Time").alias("quarter"),
    year($"Time").alias("year"),
    $"Time".cast("long").alias("id")
    )
  .distinct().toDF()

val temporaryWeather = w_weatherData.join(accdata,w_weatherData("AirPort")===accdata("AirPort"))
.select(accdata("id"),accdata("Time"),abs(accdata("time").cast("long")-w_weatherData("time").cast("long")).as("diff"))
.groupBy("id","Time")
.min("diff")


val finalData =accdata
.join(temporaryWeather,temporaryWeather("id")===accdata("id") && temporaryWeather("Time")===accdata("Time"))
.join(w_timeData,w_timeData("id")===accdata("Time").cast("long"))
.join(w_placeData,w_placeData("ZipCode")===accdata("ZipCode") && w_placeData("Street")===accdata("Street"))
.join(w_sunData,w_sunData("Sunrise_Sunset")===accdata("Sunrise_Sunset") && w_sunData("Civil_Twilight")===accdata("Civil_Twilight"))
.join(w_weatherData,w_weatherData("AirPort")===accdata("AirPort") && abs(accdata("time").cast("long")-w_weatherData("time").cast("long")) === temporaryWeather("min(diff)"))
.select(
    accdata("Is_Intersection").as("is_intersection"),
    accdata("Signal").as("is_traffic_signal"),
    accdata("Severity"),
    accdata("Distance"),
    w_timeData("id").as("time_id"),
    w_placeData("id").as("place_id"),
    w_sunData("id").as("sun_id"),
    w_weatherData("id").as("weather_id")).groupBy("is_intersection","is_traffic_signal","time_id","place_id","sun_id","weather_id").agg(count("*"),sum("Severity"),sum("Distance"))

finalData.filter(x=> x.get(6).asInstanceOf[Long]>3).show()

w_sunData.write.insertInto("w_sun")
w_placeData.write.insertInto("w_place")
w_weatherData.write.insertInto("w_weather")
w_timeData.write.insertInto("w_time")


//Wyświetlanie tabeli
spark.sql("""Select * from w_place""").show()
spark.sql("""Select * from w_time""").show()
spark.sql("""Select * from w_sun""").show()
spark.sql("""Select * from w_weather""").show()