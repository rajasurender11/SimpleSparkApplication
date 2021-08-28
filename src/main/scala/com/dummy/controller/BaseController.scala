package com.dummy.controller

import com.dummy.conf.SparkSessionConf._
import com.dummy.conf.SparkSessionConf.spark.implicits._
import com.dummy.model.{AccountsProfile, AtmTrans, Orders}
import com.dummy.util.DummyUtils
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class BaseController(args: Array[String]) {
  val log = LoggerFactory.getLogger(this.getClass)
  val configFileHDFSPath = args(0)
  val module = args(1)

  val config: Config = DummyUtils.loadProperties(configFileHDFSPath)
  val accountsProfileInputPath = config.getString("accounts_profile_input_path")
  val atmTransInputPath = config.getString("atm_trans_input_path")
  val ordersInputPath = config.getString("orders_input_path")
  val accountsProfileDelimeter = config.getString("accounts_profile_delimeter")
  val atmTransDelimeter = config.getString("atm_trans_delimeter")
  val ordersDelimeter = config.getString("orders_delimeter")
  val hiveDatabase = config.getString("hive_database_name")
  val accountsProfileTable = config.getString("accounts_profile_table_name")
  val accountsProfileOutputPath = config.getString("accounts_profile_output_path")
  val atmTransOutputPath = config.getString("atm_trans_output_path")
  val ordersOutputPath = config.getString("orders_output_path")
  val usecase1OutputPath = config.getString("usecase1_output_path")
  val usecase2OutputPath = config.getString("usecase2_output_path")
  val usecase3OutputPath = config.getString("usecase3_output_path")
  val usecase4OutputPath = config.getString("usecase4_output_path")

  def getRDD(hdfsLoc: String): RDD[String] = {
    spark.sparkContext.textFile(hdfsLoc)// records.gz
  }
/*
This method is used to create a dataframe from given RDD and delimiter of file and Case Class
 */
  def convertRDDToDataFrame(rdd: RDD[String], delimiter: String, caseClassName: String): DataFrame = {

    //log.info(s"convertRDDToDataFrame for $caseClassName with delimiter $delimiter")

    val df = caseClassName.toUpperCase() match {
      case "ACCOUNTSPROFILE" => {
        rdd.
          map(rec => rec.split(delimiter)).
          map(arr => AccountsProfile(arr(0), arr(1), arr(2), arr(3), arr(4))).
          toDF

      }
      case "ATMTRANS" => {
        rdd.
          map(rec => rec.split(delimiter)).
          map(arr => AtmTrans(arr(0), arr(1), arr(2), arr(3).toLong, arr(4))).
          toDF
      }

      case "ORDERS" => {
        rdd.
          map(rec => rec.split(delimiter)).
          map(arr => Orders(arr(0), arr(1), arr(2), arr(3), arr(4).toInt,arr(5).toInt)).
          toDF
      }
    }
    df
  }

}
