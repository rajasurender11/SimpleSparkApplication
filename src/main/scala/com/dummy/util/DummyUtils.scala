package com.dummy.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

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


}
