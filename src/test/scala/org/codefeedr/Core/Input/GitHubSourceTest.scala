package org.codefeedr.Core.Input

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.Core.Clients.GitHub.GitHubProtocol.Event
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class GitHubSourceTest extends FlatSpec with Matchers with MockFactory {

  "GitHubSource" should "create the correct GitHub API based on its Subtask Id" in {
    val source = new GitHubSource()

    //set mocking environment
    val runtimeContext = mock[RuntimeContext]
    (runtimeContext.getIndexOfThisSubtask _).expects().returns(4) //subtask 5

    //set context
    source.setRuntimeContext(runtimeContext)

    source.open(new Configuration())

    //3 keys in config, so 5 modulo 3 is key 2
    assert(source.GitHubAPI.SetOAuthToken() == "2")
  }

  "GitHubSource" should "collect the results of an event request" in {
    val source = new GitHubSource(1)

    //set mocking environment for runtimecontext
    val runtimeContext = mock[RuntimeContext]
    (runtimeContext.getIndexOfThisSubtask _).expects().returns(4) //subtask 5

    //set mocking environment for sourcecontext
    val sourceContext = mock[SourceFunction.SourceContext[Event]]
    (sourceContext.collect _).expects(*).atLeastOnce() //expect collect call at least once

    //set context
    source.setRuntimeContext(runtimeContext)

    source.open(new Configuration()) //open
    source.GitHubAPI.client.setOAuth2Token("") //set oauthtoken
    source.run(sourceContext)
  }

}
