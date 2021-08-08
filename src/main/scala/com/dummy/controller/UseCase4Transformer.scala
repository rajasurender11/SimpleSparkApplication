package com.dummy.controller

import com.dummy.conf.SparkSessionConf._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}
import org.slf4j.{Logger, LoggerFactory}
/*
Write a Spark job that simply gives you  no of customers in each bank .
The final output should be bank_name and no of customers on that bank
select bank_name ,count(*) as no_of_customers
from accounts_profile
group by bank_name
order by no_of_customers desc;
 */
class UseCase4Transformer(args: Array[String]) extends BaseController(args) {

  override val log: Logger = LoggerFactory.getLogger(this.getClass)
  def transform(accountsDF: DataFrame): DataFrame = {

   accountsDF.createOrReplaceTempView("accounts_profile")
    val df = spark.sql(
      """
        |select bankName ,count(*) as gender_count
        |from accounts_profile
        |group by bankName
        |""".stripMargin)
    df


  }

}
