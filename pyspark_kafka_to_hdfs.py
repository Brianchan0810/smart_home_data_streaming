from pyspark.sql.types import *
from pyspark.sql.functions import *

json_schema = StructType([ \
StructField('eventTimestamp', StringType(), True), \
StructField('energy', StructType([ \
StructField('consumption', DoubleType(), True), \
StructField('generation', DoubleType(), True)]), True), \
StructField('weather', StructType([ \
StructField('overall', StringType(), True), \
StructField('temperature', DoubleType(), True), \
StructField('humidity', DoubleType(), True), \
StructField('windSpeed', DoubleType(), True), \
StructField('precipIntensity', DoubleType(), True)]), True)])

raw_sdf = spark \
    .readStream \
    .format("kafka") \
    .option('kafka.bootstrap.servers','10.5.0.100:9092,10.5.0.99:9092,10.5.0.95:9092')\
    .option("kafka.security.protocol","SASL_PLAINTEXT")\
    .option("kafka.sasl.mechanism", "GSSAPI") \
    .option("kafka.sasl.kerberos.service.name", "kafka")\
	.option('subscribe','smart_home_iot_source')\
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
	
	
sdf = raw_sdf.withColumn("value", from_json(raw_sdf["value"], json_schema)).select("value.*") 

flattened_sdf = sdf \
    .selectExpr("cast(eventTimestamp as timestamp) as eventTimestamp", 
                "Energy.consumption as energyConsumption", "Energy.generation as energyGeneration",
                "Weather.overall as weatherSummary", "Weather.temperature as temperature", 
				"Weather.humidity as humidity", "Weather.windSpeed as windSpeed",
				"Weather.precipIntensity as precipIntensity") \
	.withColumn("eventDate", to_date("eventTimestamp"))


flattened_sdf.writeStream \
   .outputMode("append") \
   .partitionBy("eventDate") \
   .format("parquet") \
   .option("path", "hdfs://nameservice1/user/test_user/smart_home_iot_data") \
   .option("checkpointLocation", "hdfs://nameservice1/user/test_user/pyspark_checkpoint/load_to_hive") \
   .start()