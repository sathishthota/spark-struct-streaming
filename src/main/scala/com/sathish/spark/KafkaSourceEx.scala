package com.sathish.spark

import com.sathish.util.SparkSess
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object KafkaSourceEx extends App {

  val KAFKA_BOOTSTRAP_SERVERS = "172.27.32.199:9092"
  val KAFKA_TOPIC = "first-topic"
  val spark = SparkSess.createSession()
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
