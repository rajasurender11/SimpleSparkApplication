package com.dummy.controller

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}
import org.slf4j.{Logger, LoggerFactory}

/*
Write a Spark job that gives all the customers details apart from the TOP 3   based on their SUM of  withdrawal amounts  from any ATM's.
The O/P should not contain customer details of TOP 3. The o/p should contains all other customer details apart from the TOP 3
 The ouput should have customer account number, customer name, customer gender, customer phone no and the  total amount retrievd by him.
 */
class UseCase3Transformer(args: Array[String]) extends BaseController(args) {

  override val log: Logger = LoggerFactory.getLogger(this.getClass)

  def transform(accountsDF: DataFrame, atmTransDF: DataFrame): DataFrame = {

    val aggTransDF = atmTransDF
      .filter(col("status") === "S")
      .groupBy(col("accountId")).agg(sum("amount").as("total_amount"))

    val top3DF = aggTransDF
      .orderBy(desc("total_amount"))
      .limit(3)

    val notTop3DF = aggTransDF.alias("a")
      .join(top3DF.alias("b"), col("a.accountId") === col("b.accountId"), "leftouter")
      .where(col("b.accountId").isNull)
      .select("a.*")

    val aCols: Seq[String] = accountsDF.columns.toSeq
    val tCols: Seq[String] = notTop3DF.columns.toList.tail.toSeq
    val colsList = Seq.concat(aCols, tCols)

    accountsDF
      .join(notTop3DF,col("accountNo") === col("accountId"), "inner")
      .select(colsList.map(c => col(c)): _*)

  }

}
