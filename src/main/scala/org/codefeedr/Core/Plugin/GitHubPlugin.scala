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
import org.apache.flink.api.scala._
import org.codefeedr.Core.Clients.{COMMIT, GitHubProtocol, PUSH_EVENT}
import org.codefeedr.Core.Clients.GitHubProtocol._
import org.codefeedr.Core.Output.MongoSink

import scala.collection.JavaConversions._

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
      env.addSource(new GitHubSource(maxRequests)).filter(_.getType == "PushEvent").map { event =>
        val payload = event.getPayload.asInstanceOf[PushPayload]

        val repo = Repo(event.getRepo.getId, event.getRepo.getName, event.getRepo.getUrl)
        val actor = Actor(event.getActor.getId, event.getActor.getLogin, event.getActor.getUrl)
        val commits = payload.getCommits
          .map(
            x =>
              CommitSimple(x.getSha,
                           UserSimple(x.getAuthor.getEmail, x.getAuthor.getName),
                           x.getMessage))
          .toList

        PushEvent(
          event.getId,
          repo,
          actor,
          payload.getRef,
          payload.getSize,
          payload.getHead,
          payload.getBefore,
          commits,
          event.getCreatedAt
        )
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
