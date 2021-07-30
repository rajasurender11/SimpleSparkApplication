package com.dummy.controller

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}
import org.slf4j.{Logger, LoggerFactory}

/*
UseCase1 : Write a Spark Job that  gives all the top 3  customers details  based on their SUM of withdrawal amounts  from  any ATM's.
The output should have customer account number, customer name, customer gender, customer phone no and
the  total amount retrieved by him.
 */

class UseCase1Transformer(args: Array[String]) extends BaseController(args) {

  override val log: Logger = LoggerFactory.getLogger(this.getClass)

  def transform(accountsDF: DataFrame, atmTransDF: DataFrame): DataFrame = {

    val top3DF = atmTransDF
      .filter(col("status") === "S")
      .groupBy(col("accountId")).agg(sum("amount").as("total_amount"))
      .orderBy(desc("total_amount"))
      .limit(3)


    val aCols: Seq[String] = accountsDF.columns.toSeq
    val tCols: Seq[String] = top3DF.columns.toList.tail.toSeq
    val colsList = Seq.concat(aCols, tCols)


    accountsDF
      .join(top3DF, col("accountNo") === col("accountId"), "inner")
      .select(colsList.map(c => col(c)): _*)

  }

}
