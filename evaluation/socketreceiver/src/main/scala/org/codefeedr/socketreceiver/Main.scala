package org.codefeedr.socketreceiver

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._

object Main extends LazyLogging{
  def main(args: Array[String]): Unit = {

    val configuration = initParams(args)
    val module = new SocketReceiverModule {}
    module.socketReceiver.deploy(configuration)

  }

  /**
    * Initializes the parameterTool from the passed arguments
    * @param args arguments used to initialize parametertool
    * @return
    */
  private def initParams(args:Array[String]):ParameterTool = {
    val runIncrement = Option(System.getenv("RUN_INCREMENT"))

    if(runIncrement.isEmpty) {
      logger.warn("No run increment was defined")
    }
    val argPt = ParameterTool.fromArgs(args)

    val propertiesPath = if(argPt.has("propertiesPath")) {
      argPt.get("propertiesPath")
    } else {
      "/socketreceiver.properties"
    }
    val propertiesFile = Option(getClass.getResourceAsStream(propertiesPath)) match {
      case None => throw new IllegalArgumentException(s"Cannot find properties file (resource) with name $propertiesPath")
      case Some(p) => p
    }

    val additionalProperties = ParameterTool.fromMap(
      Map("RUN_INCREMENT"->runIncrement.getOrElse("-1")).asJava
    )

    ParameterTool.fromPropertiesFile(propertiesFile)
      .mergeWith(additionalProperties)
  }


}
