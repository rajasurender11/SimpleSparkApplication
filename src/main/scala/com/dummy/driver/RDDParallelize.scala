package com.dummy.driver
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object RDDParallelize {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkExample")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))

  }
}
