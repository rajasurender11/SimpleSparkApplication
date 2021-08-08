package com.dummy.driver

import com.dummy.controller.DummyTransformer
import org.slf4j.LoggerFactory

object DummyMain {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try {
      val dummyObject = new DummyTransformer(args)
      dummyObject.transform()
    }
    catch {
      case e: Exception => {
        log.error("Exception occurred in Spark Application : " + e.getMessage)
      }
    }
  }

}
