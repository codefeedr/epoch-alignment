package net.vankaam.flink.websocket

import akka.actor.ActorSystem
import akka.{ Done, NotUsed }
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._

import scala.concurrent.Future

/**
  * Client that performs the polls for the websocket source function
  */
class WebSocketClient(url: String) {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  /**
    * Performs a poll of the given offset and count
    * @param offset
    * @param nr
    */
  def poll(offset: Long, nr: Int): Unit = {

  }


  def close(): Unit = {

  }
}
