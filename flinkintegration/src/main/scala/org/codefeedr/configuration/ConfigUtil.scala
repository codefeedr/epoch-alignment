package org.codefeedr.configuration

import org.apache.flink.api.java.utils.ParameterTool

object ConfigUtil {
  /**
    * Tries to key the value for the given key from the parametertool
    * @param pt parameter tool to search for key
    * @param key key value to search for
    * @return value if the key exists in the parameter tool, Otherwise None
    */
  def tryGet(pt:ParameterTool, key: String):Option[String] = {
    if(pt.has(key)) {
      Some(pt.get(key))
    } else {
      None
    }
  }
}
