package com.sathish.spark

import com.sathish.util.SparkSess
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import scala.util.Random

object SparkJDBCEx extends App {

  val spark = SparkSess.createSession()
  val df1 = spark.read.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost;encrypt=true;database=master")
    .option("user", "sa")
    .option("password", "test123")
    .option("dbtable", "DB1.dbo.employee")
    .option("ssl", "True")
    .option("trustServerCertificate", "True")
    .option("sslmode", "require")
    .load()
  //df1.show(10)

  val df2 = spark.read.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost;encrypt=true;database=master")
    .option("user", "sa")
    .option("password", "test123")
    .option("dbtable", "DB2.dbo.department")
    .option("ssl", "True")
    .option("trustServerCertificate", "True")
    .option("sslmode", "require")
    .load()
  //df2.show(10)

  val df3 = df1.join(df2, df1("deptno") === df2("id"), "inner")//.filter(df1("deptno")==="1002")
    .select(df1("id").as("Emp_ID"),
      df1("name").as("Emp_Name"),
      df1("deptno").as("Dept_Id"),
      df2("name").as("Dept_Name")).withColumn("Salary", lit(Random.nextInt(100000)))
  //df3.show(20)

  df3.write.format("jdbc")
    .option("url", "jdbc:sqlserver://localhost;encrypt=true;database=master")
    .option("user", "sa")
    .option("password", "test123")
    .option("dbtable", "DB1.dbo.employeeNew")
    .option("ssl", "True")
    .option("trustServerCertificate", "True")
    .mode("append")
    .option("sslmode", "require").save()

}