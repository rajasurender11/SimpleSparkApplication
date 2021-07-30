package com.dummy.controller

import com.dummy.util.DummyUtils
import org.slf4j.LoggerFactory


class DummyTransformer(args: Array[String]) extends BaseController(args) {

  override val log = LoggerFactory.getLogger(this.getClass)

  def transform(): Unit = {

    val accountsRDD = getRDD(accountsProfileInputPath)
    val atmTransRDD = getRDD(atmTransInputPath)
    val ordersRDD = getRDD(ordersInputPath)

    val accountsDF = convertRDDToDataFrame(accountsRDD, accountsProfileDelimeter, "AccountsProfile")
    val atmTransDF = convertRDDToDataFrame(atmTransRDD, atmTransDelimeter, "AtmTrans")
    val ordersDF = convertRDDToDataFrame(ordersRDD, ordersDelimeter, "Orders")

    val useCaseDF = module.toUpperCase() match {
      case "USECASE1" => new UseCase1Transformer(args).transform(accountsDF, atmTransDF)
      case _ => ordersDF
    }
    
    useCaseDF.show(100, false)


    module.toUpperCase() match {
      case "USECASE1" => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase1OutputPath)
      case "USECASE2" => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase1OutputPath)
      case _ => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase1OutputPath)
    }



  }

}
