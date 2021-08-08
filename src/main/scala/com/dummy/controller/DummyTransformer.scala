package com.dummy.controller

import com.dummy.util.DummyUtils
import org.apache.spark.storage.StorageLevel
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
      case "USECASE2" => new UseCase2Transformer(args).transform(accountsDF, atmTransDF)
      case "USECASE3" => new UseCase3Transformer(args).transform(accountsDF, atmTransDF)
      case "USECASE4" => new UseCase4Transformer(args).transform(accountsDF)
      case _ => ordersDF
    }
    useCaseDF.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    useCaseDF.show(100, false)

    module.toUpperCase() match {
      case "USECASE1" => DummyUtils.saveToHDFS(useCaseDF, "parquet", "OverWrite", usecase1OutputPath)
      case "USECASE2" => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase2OutputPath)
      case "USECASE3" => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase3OutputPath)
      case "USECASE4" => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase4OutputPath)
      case _ => DummyUtils.saveToHDFS(useCaseDF, "avro", "OverWrite", usecase1OutputPath)
    }
    useCaseDF.unpersist()
  }

}
