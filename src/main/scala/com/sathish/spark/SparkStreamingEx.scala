package com.sathish.spark

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SparkStreamingEx extends App{

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.OFF)
  import org.apache.hadoop.security.UserGroupInformation

  UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"))
  val spark = SparkSession.builder().appName("ANewUnitTest").master("local").getOrCreate()
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
