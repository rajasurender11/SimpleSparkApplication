package com.dummy.driver
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write



object JsonLoaderMain {

  def main(args: Array[String]): Unit = {

    val customJson ="""{
          "empId":"100",
          "empSkills": ["hadoop", "spark"]
        }"""


    val jsonObject = parse(customJson).extract[JObject]

    val empId = (jsonObject \ "empId").extract[String]
    val skillArray = (jsonObject \ "empSkills").extract[Array[String]]




  }

}
