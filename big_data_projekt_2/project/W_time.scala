import org.apache.spark.sql._
import org.apache.spark.sql.functions.{dayofmonth, hour, month, quarter, year}
import java.time.LocalDateTime
import java.sql.Timestamp

object W_time {

  def getStartDate(startDate: Timestamp): LocalDateTime = {
    val date = startDate.toLocalDateTime().withMinute(0).withSecond(0)
    val hour = date.getHour()
    if (hour >= 21)
      return date.withHour(21)
    else if (hour >= 18)
      return date.withHour(18)
    else if (hour >= 15)
      return date.withHour(15)
    else if (hour >= 12)
      return date.withHour(12)
    else if (hour >= 9)
      return date.withHour(9)
    else if (hour >= 6)
      return date.withHour(6)
    else if (hour >= 3)
      return date.withHour(3)
    date.withHour(0)
  }

  def getTimeData(startTime:Timestamp):List[Timestamp] ={
    List(startTime)
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

    val mainData = mainDataCentral
      .union(mainDataEastern)
      .union(mainDataMountain)
      .union(mainDataPacific)
      .dropDuplicates(Array("ID"))

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
      .withColumnRenamed("_9", "NumberOfParts")
      .withColumnRenamed("_10", "Time")

    val w_timeData = accdata.select(
      hour($"Time").alias("hour"),
      dayofmonth($"Time").alias("day"),
      month($"Time").alias("month"),
      quarter ($"Time").alias("quarter"),
      year($"Time").alias("year"),
      $"Time".cast("long").alias("id")
    )
      .distinct().toDF()
    w_timeData.write.insertInto("w_time")
  }
}
