package com.sathish.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object KafkaSourceEx extends App {

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  val KAFKA_BOOTSTRAP_SERVERS = "172.27.32.199:9092"
  val KAFKA_TOPIC = "first-topic"

  Logger.getLogger("org").setLevel(Level.OFF)
  import org.apache.hadoop.security.UserGroupInformation

 UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"))
  val spark = SparkSession.builder().appName("KafkaStream").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
  df.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
  .awaitTermination()
}
