
# Skrypt do stawiania klastra
gcloud beta dataproc clusters create ${CLUSTER_NAME}  --enable-component-gateway --bucket ${BUCKET_NAME}  --region europe-west3 --zone europe-west3-b  --master-machine-type n1-standard-2 --master-boot-disk
-size 50  --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50  --image-version 1.3-debian9 --optional-components ZEPPELIN  --project ${PROJECT_ID} --max-age=3h

# Import danych do hadoop
hadoop fs -copyToLocal gs://big_data_kowalczyk/projekt2/us-accidents.zip
unzip us-accidents.zip 
hadoop fs -mkdir -p us-accidents
hadoop fs -copyFromLocal us-accidents/*.* us-accidents
hadoop fs -ls us-accidents


val user = "TWOJA_NAZWA_USERA"
// Import danych z csv do sparka
val mainDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataCentral.csv").cache();
val mainDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataEastern.csv").cache();
val mainDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataMountain.csv").cache();
val mainDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/mainDataPacific.csv").cache();
val geoDataCentral = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataCentral.csv").cache();
val geoDataEastern = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataEastern.csv").cache();
val geoDataMountain = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataMountain.csv").cache();
val geoDataPacific = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(s"/user/"+user+"/us-accidents/geoDataPacific.csv").cache();
val weatherToParse = spark.read.text(s"/user/"+user+"/us-accidents/weather.txt")
