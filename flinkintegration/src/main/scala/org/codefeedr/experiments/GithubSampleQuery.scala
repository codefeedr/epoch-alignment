package org.codefeedr.experiments

import org.apache.flink.streaming.api.windowing.assigners.{
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.codefeedr.experiments.model.UserProject
import org.codefeedr.plugins.github.generate.{CommitGenerator, ProjectGenerator, UserGenerator}
import org.apache.flink.streaming.api.scala._

/**
  * Sample query using generated github data
  */
object GithubSampleQuery extends ExperimentBase {
  val seed1 = 1035940093935715931L
  val seed2 = 5548464088400911859L
  val seed3 = 3985731179907005257L

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    initialize(args)
    val env = getEnvironment

    // val commits = env.addSource(createGeneratorSource((l:Long) => new CommitGenerator(l),seed1,"CommitGenerator"))
    val users = env.addSource(
      createGeneratorSource((l: Long) => new UserGenerator(l), seed2, "UserGenerator"))
    val projects = env.addSource(
      createGeneratorSource((l: Long) => new ProjectGenerator(l), seed3, "ProjectGenerator"))

    val userProjects =
      users
        .join(projects)
        .where(o => o.id)
        .equalTo(o => o.owner_id)
        .window(TumblingProcessingTimeWindows.of(getWindowTime))
        .apply(
          (user, project) =>
            UserProject(
              userId = user.id,
              projectId = project.id,
              userLogin = user.login,
              projectDescription = project.description
          ))

    userProjects.addSink(o => Unit)

    env.execute("userProjectGenerator")
  }

}
