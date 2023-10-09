package com.sathish.spark

import com.sathish.util.SparkSess
import org.apache.spark.sql.SparkSession

object ParquetSparkEx extends App{

  val spark = SparkSess.createSession()
  val df = spark.read.parquet("C:\\Users\\sathi\\spark\\")
  println(df.count())
  println(df.distinct().count())

  /*df.write.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost;encrypt=true;database=master")
    .option("user", "sa")
    .option("password", "test123")
    .option("dbtable", "DB1.dbo.userdata")
    .option("ssl", "True")
    .option("trustServerCertificate", "True")
    .mode("append")
    .option("sslmode", "require").save()*/
}
