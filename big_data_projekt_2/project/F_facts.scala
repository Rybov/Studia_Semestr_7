import org.apache.spark.sql._
import org.apache.spark.sql.functions.{abs, count, dayofmonth, hour, monotonically_increasing_id, month, quarter, sum, year}

import java.sql.Timestamp
import java.text.SimpleDateFormat

object F_facts {

  def getTimeData(startTime:Timestamp):List[Timestamp] ={
    List(startTime)
  }

  def getDateTime(dateTime: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy-dd-MM' 'HH:mm:ss")
    val x = format.parse(dateTime.dropRight(2))
    new Timestamp(x.getTime())
  }

  def getCondition(line:String):String = {
    line.split("Condition: ")(1)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    val user = args(0)
    val mainDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataCentral.csv").cache();
    val mainDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataEastern.csv").cache();
    val mainDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataMountain.csv").cache();
    val mainDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataPacific.csv").cache();
    val geoDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataCentral.csv").cache();
    val geoDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataEastern.csv").cache();
    val geoDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataMountain.csv").cache();
    val geoDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataPacific.csv").cache();
    val weatherToParse = spark.read.text(s"/user/"+user+"/us-accidents/weather.txt")

    val mainData = mainDataCentral
      .union(mainDataEastern)
      .union(mainDataMountain)
      .union(mainDataPacific)
      .dropDuplicates(Array("ID"))

    val geoData = geoDataCentral
      .union(geoDataEastern)
      .union(geoDataMountain)
      .union(geoDataPacific)
      .dropDuplicates(Array("Zipcode"))

    val accdata = mainData.flatMap(row => {
      val list = getTimeData(row.get(4).asInstanceOf[Timestamp])
      list.map(x => (
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
        1.0/list.length,
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
      .withColumnRenamed("_11", "NumberOfParts")
      .withColumnRenamed("_12", "Time")

    val w_timeData = accdata.select(
      hour($"Time").alias("hour"),
      dayofmonth($"Time").alias("day"),
      month($"Time").alias("month"),
      quarter ($"Time").alias("quarter"),
      year($"Time").alias("year"),
      $"Time".cast("long").alias("id")
    )
      .distinct().toDF()

    val w_placeData = geoData.join(mainData, mainData("ZipCode") === geoData("ZipCode"))
      .select(mainData("Street"), geoData("City"), geoData("County"), geoData("State"),geoData("Zipcode"))
      .distinct()
      .withColumn("id", monotonically_increasing_id+1)

    val w_sunData = accdata.select($"Sunrise_Sunset", $"Civil_Twilight")
      .distinct()
      .withColumn("id", monotonically_increasing_id+1)

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
    val w_weatherData = weather.distinct().withColumn("id", monotonically_increasing_id+1)
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
        accdata("NumberOfParts"),
        w_timeData("id").as("time_id"),
        w_placeData("id").as("place_id"),
        w_sunData("id").as("sun_id"),
        w_weatherData("id").as("weather_id"))
      .groupBy("is_intersection","is_traffic_signal","time_id","place_id","sun_id","weather_id").agg(count("*"),sum("Severity"),sum("Distance"),sum("NumberOfParts"))
      .withColumn("id", monotonically_increasing_id+1)
      .withColumnRenamed("sum(Severity)", "sum_severity")
      .withColumnRenamed("sum(Distance)", "sum_distance ")
      .withColumnRenamed("sum(NumberOfParts)", "sum_parts_accidents")
      .withColumnRenamed("count(1)", "accidents_per_three_hour")
    finalData.write.insertInto("f_facts")
  }
}
