package org.codefeedr.Core.Plugin

import java.util.Date
import java.util.concurrent.TimeUnit

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
import scala.collection.JavaConversions._
import org.apache.flink.api.scala._
import org.codefeedr.Core.Clients.GitHubProtocol
import org.codefeedr.Core.Clients.GitHubProtocol.{Commit, PushEvent}
import org.codefeedr.Core.Output.MongoSink

class GitHubPlugin[Commit: ru.TypeTag: ClassTag](maxRequests: Integer = -1)
    extends AbstractPlugin {

  /**
    * Creates a new SubjectType.
    * @return
    */
  override def CreateSubjectType(): SubjectType = {
    return SubjectTypeFactory.getSubjectType[Commit]

  }

  def GetStream(env: StreamExecutionEnvironment): DataStream[GitHubProtocol.Commit] = {
    val stream =
      env.addSource(new GitHubSource(maxRequests)).filter(_.getType == "PushEvent").map { event =>
        val payload = event.getPayload.asInstanceOf[PushPayload]

        PushEvent(event.getId,
                  event.getRepo.getName,
                  payload.getRef,
                  payload.getBefore,
                  payload.getHead,
                  new Date(event.getCreatedAt.getTime))
      }

    val finalStream = AsyncDataStream.unorderedWait(stream,
                                                    new GHRetrieveCommitFunction,
                                                    1000,
                                                    TimeUnit.MILLISECONDS,
                                                    100)

    finalStream
  }

  override def Compose(env: StreamExecutionEnvironment): Future[Unit] = async {
    val sink = await(SubjectFactory.GetSink[GitHubProtocol.Commit])
    val stream = GetStream(env)
    stream.addSink(new MongoSink("commits", "id", "repo_name"))
    stream.addSink(sink)
  }
}
