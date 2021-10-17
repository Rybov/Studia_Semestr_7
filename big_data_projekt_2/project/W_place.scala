import org.apache.spark.sql._
import org.apache.spark.sql.functions.monotonically_increasing_id

object W_place {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val user = args(0)
    val mainDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataCentral.csv").cache();
    val mainDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataEastern.csv").cache();
    val mainDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataMountain.csv").cache();
    val mainDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataPacific.csv").cache();
    val geoDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataCentral.csv").cache();
    val geoDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataEastern.csv").cache();
    val geoDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataMountain.csv").cache();
    val geoDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataPacific.csv").cache();

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

    val w_placeData = geoData.join(mainData, mainData("ZipCode") === geoData("ZipCode"))
      .select(mainData("Street"), geoData("City"), geoData("County"), geoData("State"),geoData("Zipcode"))
      .distinct()
      .withColumn("id", monotonically_increasing_id+1)
    w_placeData.write.insertInto("w_place")
  }
}
