package com.dummy.util

import com.dummy.driver.ArgumentsLoaderMain.OptionMap

object ArgsLoader {

  def loadArgsAsMap(map: OptionMap, argsList: List[String]): OptionMap = {

    // "clientName","sweden","runId", "abcd-defg"
    argsList match {
      case Nil => map
      case "--clientName" :: value  :: tail => loadArgsAsMap(map ++ Map('clientName -> value.toString), tail)
      case "--fileName" :: value :: tail => loadArgsAsMap(map ++ Map('fileName -> value.toString), tail)
      case "--runId" :: value :: tail => loadArgsAsMap(map ++ Map('runId -> value.toString), tail)
      case "--cityName" :: value :: tail => loadArgsAsMap(map ++ Map('cityName -> value.toString), tail)
      case option :: tail => println("unknown option " + option); null

    }

  }

}
