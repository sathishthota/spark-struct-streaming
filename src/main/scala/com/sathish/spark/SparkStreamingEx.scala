package com.sathish.spark

import com.sathish.util.SparkSess
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SparkStreamingEx extends App{

  val spark = SparkSess.createSession()
  val userSchema = new StructType().add("name", "string")
  val df = spark.readStream
    .schema(userSchema)//Below codes provides example
    .csv("C:\\\\Users\\\\sathi\\\\working\\\\filestream")


  df.writeStream
    .format("console")
    .outputMode("update")
    .start()             // Start the computation
    .awaitTermination()

}
