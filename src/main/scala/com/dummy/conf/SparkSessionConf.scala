package com.dummy.conf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSessionConf {

  val spark = SparkSession.builder
    .appName("SparkExample")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .enableHiveSupport()
    .getOrCreate



}
