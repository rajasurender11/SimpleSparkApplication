package com.dummy.driver

import com.dummy.controller.WindowsFunctionController
import org.slf4j.LoggerFactory


object WindowsFunctionMain {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try {
      val winObj = new WindowsFunctionController(args)
      winObj.transform()
    }
    catch {
      case e: Exception => {
        log.error("Exception occurred in Spark Application : " + e.getMessage)
      }
    }
  }

}
