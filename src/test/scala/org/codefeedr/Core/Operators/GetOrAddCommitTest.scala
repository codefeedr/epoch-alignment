package org.codefeedr.Core.Operators

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.scalatest.tagobjects.Slow
import org.mockito.Mockito._

import scala.async.Async._

class GetOrAddCommitTest extends MongoDBSpec {

  "The correct indexes" should "be set when the GetOrAddCommit is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddCommit()
    await(operator.SetIndexes(operator.GetIndexNames))

    val indexes = await {
      operator.
        mongoDB.
        getCollection(operator.GetCollectionName).
        listIndexes().toFuture()
    }

    //find the correct index
    val findIndex = indexes.flatMap(_.find(x => x._1 == "key" && x._2.asDocument().containsKey("url")))
    assert(findIndex.size == 1)
  }

  "The correct API key" should "be set when GetOrAddCommit is initialized" taggedAs(Slow) in async {
    val operator = new GetOrAddCommit()
    val runtimeContext = mock[RuntimeContext]

    //subtask 3
    when(runtimeContext.getIndexOfThisSubtask).thenReturn(2)

    //set runtime context
    operator.setRuntimeContext(runtimeContext)

    //open operator
    operator.open(new Configuration())

    //assert the 3th token is picked (3 modulo 3 == 0)
    assert(operator.GitHubAPI.SetOAuthToken() == "0")
  }



}
