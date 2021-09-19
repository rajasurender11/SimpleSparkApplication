package com.dummy.schema


import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AccountsProfileSchema  {

  val fileLoc = "/user/training/surender_hadoop/input_files/accounts_profile/accounts_profile.txt"
  val schema = StructType(Array(
    StructField("accountNo", StringType, true),
    StructField("bankName", StringType, true),
    StructField("custName", StringType, true),
    StructField("gender", StringType, true),
    StructField("phNo", StringType, true)
  ))

}
