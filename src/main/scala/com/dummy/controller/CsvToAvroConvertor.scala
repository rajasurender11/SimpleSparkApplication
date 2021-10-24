package com.dummy.controller

import com.dummy.schema.{AccountsProfileSchema, AtmTransSchema}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import com.dummy.conf.SparkSessionConf.spark
import com.dummy.util.DummyUtils

class CsvToAvroConvertor(args: Array[String]) extends BaseController(args) {

  override val log = LoggerFactory.getLogger(this.getClass)

  def transform(): Unit = {

    val csvFilesSchemaMap = Map(
      (AccountsProfileSchema.fileLoc, AccountsProfileSchema.schema),
      (AtmTransSchema.fileLoc, AtmTransSchema.schema)
    )

    val fs:FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var fileCount = 0
    val expectedFileCount = csvFilesSchemaMap.size

    csvFilesSchemaMap.keys.foreach(key => {
      val isFileExists = fs.exists(new Path(key))
      if(isFileExists) fileCount +=1 else log.info(s"HDFS file $key does not exists!!")
    })

       if(fileCount == expectedFileCount )
        {

          log.info(s"All CSV files available in HDFS, Converting to AVRO")
          csvFilesSchemaMap.keys.foreach(key => {
            val schema = csvFilesSchemaMap(key)
            val df = DummyUtils.readCsvWithSchema(key,schema)
            DummyUtils.saveToHDFS(df, "avro", "OverWrite", usecase1OutputPath)
          })
        }
      else {

      }

  }


}
