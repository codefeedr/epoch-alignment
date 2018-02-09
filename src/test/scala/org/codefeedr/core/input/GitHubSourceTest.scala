package org.codefeedr.core.input

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.core.clients.github.GitHubProtocol.Event
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.scalatest.{FlatSpec, Matchers}

class GitHubSourceTest extends FlatSpec with Matchers with MockitoSugar {

  "GitHubSource" should "create the correct GitHub API based on its Subtask Id" in {
    val source = new GitHubSource()

    //set mocking environment
    val runtimeContext = mock[RuntimeContext]

    //return 4 when asked (subtask 5)
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(4)

    //set context
    source.setRuntimeContext(runtimeContext)

    source.open(new Configuration())

    //verify called once
    verify(runtimeContext, times(1)).getIndexOfThisSubtask

    //3 keys in config, so 5 modulo 3 is key 2
    assert(source.gitHubAPI.setOAuthToken() == "2")
  }

  "GitHubSource" should "collect the results of an event request" in {
    val source = new GitHubSource(1)

    //set mocking environment for runtimecontext
    val runtimeContext = mock[RuntimeContext]

    //return 4 when asked (subtask 5)
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(4)

    //set mocking environment for sourcecontext
    val sourceContext = mock[SourceFunction.SourceContext[Event]]

    //set context
    source.setRuntimeContext(runtimeContext)

    source.open(new Configuration()) //open
    source.gitHubAPI.client.setOAuth2Token("") //set oauthtoken
    source.run(sourceContext)

    verify(sourceContext, atLeastOnce()).collect(any[Event]) //expect collect call at least once
  }

}
