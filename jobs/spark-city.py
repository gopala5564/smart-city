from ctypes import Structure
from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", configuration['AWS_ACCESS_KEY'])\
            .config("spark.hadoop.fs.s3a.access.key", configuration['AWS_SECRET_KEY'])\
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
            .getOrCreate()
    
    #adjust a log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')
    #vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    #gpsSchema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    #trafficschema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("cameraId", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("snapshot", StringType(), True),
    ])

    #weatherschema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("humidity", IntegerType(), True),\
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    #emergencyschema

    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr('CAST(values) AS STRING')
                .select(from_json(col('values'), schema).alias('data'))
                .select('data.*')
                .withWaterMark('timestamp', '2 minutes')
        )
    

    def streamwriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    

    vehicleDF = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gps_schema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    #join all the dfs with id and timestamp

    query1 = streamwriter(vehicleDF, 's3a://streaming-spark-data/checkpoints/vehicle_data',
                's3a://streaming-spark-data/data/vehicle_data')
    query2 = streamwriter(gpsDF, 's3a://streaming-spark-data/checkpoints/gps_data',
                's3a://streaming-spark-data/data/gps_data')
    query3 = streamwriter(trafficDF, 's3a://streaming-spark-data/checkpoints/traffic_data',
                's3a://streaming-spark-data/data/traffic_data')
    query4 = streamwriter(weatherDF, 's3a://streaming-spark-data/checkpoints/weather_data',
                's3a://streaming-spark-data/data/weather_data')
    query5 = streamwriter(emergencyDF, 's3a://streaming-spark-data/checkpoints/emergency_data',
                's3a://streaming-spark-data/data/emergency_data')

    query5.awaitTermination()

if __name__=="__main__":
    main()