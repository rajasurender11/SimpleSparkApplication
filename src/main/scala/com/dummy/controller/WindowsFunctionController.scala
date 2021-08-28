package com.dummy.controller

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class WindowsFunctionController(args: Array[String]) extends  BaseController(args) {

  def transform(): Unit = {

    val rankingFunction = args(1)

    val atmTransRDD = getRDD(atmTransInputPath)
    val atmTransDF = convertRDDToDataFrame(atmTransRDD, atmTransDelimeter, "AtmTrans")
    val windowSpec  = Window.partitionBy("accountId").orderBy(desc("transDt"))

   val rankedDF = rankingFunction match {
      case "ROW_NUMBER" => atmTransDF.withColumn("uniqueNumber",row_number.over(windowSpec))
      case "RANK" => atmTransDF.withColumn("uniqueNumber",rank().over(windowSpec))
      case "DENSE_RANK" => atmTransDF.withColumn("uniqueNumber",dense_rank().over(windowSpec))
      case _ => atmTransDF
    }

    rankedDF.cache() // As i am using the same rankedDF  in two actions, i have cached it

    rankedDF.orderBy(col("accountId").asc,col("uniqueNumber")).show(100,false)

    rankedDF.filter(col("uniqueNumber") === 1).show(100,false)

  }

}
