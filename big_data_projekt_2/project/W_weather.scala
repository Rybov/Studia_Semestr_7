import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions.monotonically_increasing_id

object W_weather {

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
    val weatherToParse = spark.read.text(s"/user/"+user+"/us-accidents/weather.txt")
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
    w_weatherData.write.insertInto("w_weather")
  }
}

