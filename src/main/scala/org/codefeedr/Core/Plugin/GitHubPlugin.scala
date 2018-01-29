package org.codefeedr.Core.Plugin

import java.util.Date
import java.util.concurrent.TimeUnit

import com.google.gson.{Gson, GsonBuilder, JsonObject}
import org.codefeedr.Core.Input.{GHRetrieveCommitFunction, GitHubSource}
import org.apache.flink.streaming.api.scala.{
  AsyncDataStream,
  DataStream,
  StreamExecutionEnvironment
}
import org.codefeedr.Core.Library.Internal.{AbstractPlugin, SubjectTypeFactory}
import org.codefeedr.Core.Library.SubjectFactory
import org.codefeedr.Model.SubjectType
import org.eclipse.egit.github.core.event.PushPayload

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import org.apache.flink.api.scala._
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.{Actor, Payload, PushEvent, Repo}
import org.codefeedr.Core.Clients.MongoDB.PUSH_EVENT
import org.codefeedr.Core.Output.MongoSink

import scala.collection.JavaConverters._

class GitHubPlugin[PushEvent: ru.TypeTag: ClassTag](maxRequests: Integer = -1)
    extends AbstractPlugin {

  /**
    * Creates a new SubjectType.
    * @return
    */
  override def CreateSubjectType(): SubjectType = {
    return SubjectTypeFactory.getSubjectType[PushEvent]

  }

  def GetStream(env: StreamExecutionEnvironment): DataStream[GitHubProtocol.PushEvent] = {
    val stream =
      env.addSource(new GitHubSource(maxRequests)).filter(_.`type` == "PushEvent").map { x =>
        val gson = new Gson()
        val payload = gson.fromJson(x.payload, classOf[GitHubProtocol.Payload])
        PushEvent(x.id, x.repo, x.actor, payload, x.public, x.created_at)
      }
    stream
  }

  override def Compose(env: StreamExecutionEnvironment): Future[Unit] = async {
    val sink = await(SubjectFactory.GetSink[GitHubProtocol.PushEvent])
    val stream = GetStream(env)
    stream.addSink(sink)
    stream.addSink(new MongoSink[GitHubProtocol.PushEvent](PUSH_EVENT, "id"))
  }

}
