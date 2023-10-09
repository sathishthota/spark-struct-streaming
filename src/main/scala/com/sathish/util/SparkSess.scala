package com.sathish.util

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.security.UserGroupInformation

object SparkSess {

  def createSession(): SparkSession ={
    Logger.getLogger("org").setLevel(Level.OFF)

    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"))

    val spark = SparkSession.builder().appName("ParquettoDB").master("local").getOrCreate()
    spark
  }
}
