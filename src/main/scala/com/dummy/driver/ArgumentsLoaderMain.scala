package com.dummy.driver

import com.dummy.util.ArgsLoader
import org.slf4j.LoggerFactory

object ArgumentsLoaderMain {
  val log = LoggerFactory.getLogger(this.getClass)
  type OptionMap = Map[Symbol,Any]

  def main(args: Array[String]): Unit = {

  //val  args = Array("--clientName","sweden","--runId", "abcd-defg")

    val argsList = args.toList
    val argsMap = ArgsLoader.loadArgsAsMap(Map(),argsList)
    println(argsMap('clientName).toString)
    println(argsMap)

  }

}
