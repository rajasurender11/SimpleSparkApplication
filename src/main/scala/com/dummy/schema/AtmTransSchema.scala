package com.dummy.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AtmTransSchema {
  val fileLoc = "/user/training/surender_hadoop/input_files/atm_trans/atm_trans.txt"
  val schema = StructType(Array(
    StructField("accountId", StringType, true),
    StructField("atmId", StringType, true),
    StructField("transDt", StringType, true),
    StructField("amount", StringType, true),
    StructField("status", StringType, true)
  ))

}
