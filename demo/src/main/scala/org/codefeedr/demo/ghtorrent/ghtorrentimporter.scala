package org.codefeedr.demo.ghtorrent

import net.vankaam.flink.WebSocketSourceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.plugin.SimplePlugin
import org.codefeedr.ghtorrent.User
import org.apache.flink.streaming.api.scala._
import org.codefeedr.serde.ghtorrent._
import org.codefeedr.demo.ghtorrent.Serde.ops._

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.{universe => ru}

class WebSocketJsonPlugin[TData: ru.TypeTag: ClassTag](url: String,
                                                       subject: String,
                                                       batchSize: Int)
    extends SimplePlugin[TData] {
  @transient private lazy val targetType = classTag[TData].runtimeClass.asInstanceOf[Class[TData]]
  @transient implicit lazy val typeInfo: TypeInformation[TData] = TypeInformation.of(targetType)
  @transient private lazy val source = WebSocketSourceFunction(url, subject, batchSize)

  /**
    * Method to implement as plugin to expose a datastream
    * Make sure this implementation is serializable!
    *
    * @param env The environment to create the datastream on
    * @return The datastream itself
    */
  override def getStream(env: StreamExecutionEnvironment): DataStream[TData] =
    env
      .addSource(source)
      .map((o: String) => o.deserialize)
}

/**
  * Main class for a simple job that reads data from a websocket,
  * deserializes it and passes it as a kafka subject
  */
object GhTorrentUserImporter {

  def main(args: Array[String]): Unit = {
    val parameter = ParameterTool.fromArgs(args)
    val url = parameter.getRequired("url")
    val subjectName = parameter.getRequired("subject")
    val batchSize = parameter.getInt("batchSize", 100)

    val plugin = new WebSocketJsonPlugin[User](url, subjectName, batchSize)
    Await.ready(plugin.reCreateSubject(), 5.seconds)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //@transient implicit lazy val formats: DefaultFormats.type = DefaultFormats
    //env.addSource(socket).map[User]((o:String) => parse(o).extract[User])

    Await.result(plugin.compose(env, "readusers"), 5.seconds)
    env.execute()
  }
}

class WebSocketPlugin(url: String, subject: String, batchSize: Int) extends SimplePlugin[String] {
  @transient private lazy val source = WebSocketSourceFunction(url, subject, batchSize)

  override def getStream(env: StreamExecutionEnvironment): DataStream[String] =
    env.addSource(source)
}
