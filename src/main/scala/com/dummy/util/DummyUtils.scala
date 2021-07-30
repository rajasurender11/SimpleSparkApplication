package com.dummy.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import com.databricks.spark.avro._

import java.io.{FileNotFoundException, InputStreamReader}

object DummyUtils {

  val log = LoggerFactory.getLogger(this.getClass)
  val conf = new Configuration()

  def loadProperties(configLoc: String): Config = {
    try {
      val path = new Path(configLoc)
      val fs = FileSystem.get(path.toUri, conf)
      ConfigFactory.parseReader(new InputStreamReader(fs.open(path)))
    }
    catch {
      case f: FileNotFoundException => {
        log.error(s"Properties file not found in HDFS loc : $configLoc")
        throw f
      }
      case e: Exception => {
        log.error(s"Error Loading Config File : $configLoc")
        throw e
      }
    }

  }

  def saveToHDFS(df: DataFrame, formatString: String, saveModeString: String, hdfsOutputLoc: String): Unit = {

    val format = formatString match {
      case "avro" => "com.databricks.spark.avro"
      case "parquet" => "parquet"
      case _ => "parquet"
    }

    val mode: SaveMode = saveModeString match {
      case "Append" => SaveMode.Append
      case "OverWrite" => SaveMode.Overwrite
      case _ => SaveMode.Append
    }

    log.info(s" Writing dataframe to hdfs loc $hdfsOutputLoc with ${mode.toString}... ")
    df.write.format(format).mode(mode).save(hdfsOutputLoc)
  }


}
