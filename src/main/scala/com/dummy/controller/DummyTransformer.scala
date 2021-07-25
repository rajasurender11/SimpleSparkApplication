package com.dummy.controller

import org.slf4j.LoggerFactory
import com.dummy.conf.SparkSessionConf._


class DummyTransformer(args: Array[String]) extends BaseController(args) {

  override val log = LoggerFactory.getLogger(this.getClass)

  def transform(): Unit = {

    val accountsRDD = getRDD(accountsProfileInputPath)
    val atmTransRDD = getRDD(atmTransInputPath)
    val ordersRDD = getRDD(ordersInputPath)

    val accountsDF = convertRDDToDataFrame(accountsRDD, accountsProfileDelimeter, "AccountsProfile")
    val atmTransDF = convertRDDToDataFrame(atmTransRDD, atmTransDelimeter, "AtmTrans")
    val ordersDF = convertRDDToDataFrame(ordersRDD, ordersDelimeter, "Orders")



  }

}
