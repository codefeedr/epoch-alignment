package org.codefeedr.Core.input

import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector
import org.codefeedr.Core.Plugin.PushEvent
import org.eclipse.egit.github.core.Commit

//TODO Change types
class GHRetrieveCommitFunction extends AsyncFunction[PushEvent, Commit] {

  override def asyncInvoke(input: PushEvent, collector: AsyncCollector[Commit]): Unit = {
    //TODO retrieve correct commits (maybe also add a MongoConnection here
  }
}
