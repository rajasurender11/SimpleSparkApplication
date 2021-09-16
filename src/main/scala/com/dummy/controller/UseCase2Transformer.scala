package com.dummy.controller

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, sum}
import org.slf4j.{Logger, LoggerFactory}

/*
UseCase1 : Write a Spark Job that  gives all the bottom  3  customers details  based on their SUM of withdrawal amounts  from  any ATM's.
The output should have customer account number, customer name, customer gender, customer phone no and
the  total amount retrieved by him.
 */
class UseCase2Transformer(args: Array[String]) extends BaseController(args) {
  override val log: Logger = LoggerFactory.getLogger(this.getClass)

  def transform(accountsDF: DataFrame, atmTransDF: DataFrame): DataFrame = {

  val bottom3DF = atmTransDF
    .filter(col("status") === "S")
    .groupBy(col("accountId")).agg(sum("amount").as("total_amount"))
    .orderBy(asc("total_amount"))
    .limit(3)

    atmTransDF.filter(atmTransDF("status") === "S")


  val aCols: Seq[String] = accountsDF.columns.toSeq
  val tCols: Seq[String] = bottom3DF.columns.toSeq.tail
  val colsList = Seq.concat(aCols, tCols)

  accountsDF
    .join(bottom3DF, col("accountNo") === col("accountId"), "inner")
    .select(colsList.map(c => col(c)): _*)
}

}
