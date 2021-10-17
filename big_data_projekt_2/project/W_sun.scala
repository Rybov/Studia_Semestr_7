import org.apache.spark.sql._
import org.apache.spark.sql.functions.monotonically_increasing_id

object W_sun {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    val user = args(0)
    val mainDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/" + user + "/us-accidents/mainDataCentral.csv").cache();
    val mainDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/" + user + "/us-accidents/mainDataEastern.csv").cache();
    val mainDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/" + user + "/us-accidents/mainDataMountain.csv").cache();
    val mainDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/" + user + "/us-accidents/mainDataPacific.csv").cache();

    val mainData = mainDataCentral
      .union(mainDataEastern)
      .union(mainDataMountain)
      .union(mainDataPacific)
      .dropDuplicates(Array("ID"))

    val accdata = mainData.flatMap(row => List(row.get(30).asInstanceOf[String], row.get(31).asInstanceOf[String]))
      .withColumnRenamed("_1", "Sunrise_Sunset")
      .withColumnRenamed("_2", "Civil_Twilight")

    val w_sunData = accdata.select($"Sunrise_Sunset", $"Civil_Twilight")
      .distinct()
      .withColumn("id", monotonically_increasing_id+1)
    w_sunData.write.insertInto("w_sun")
  }
}
