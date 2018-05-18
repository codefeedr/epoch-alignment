package org.codefeedr.core.library.internal.kafka.sink

import org.codefeedr.core.library.metastore.{EpochCollectionNode, EpochNode}
import org.codefeedr.model.zookeeper.Partition
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

import scala.concurrent.Future

case class EpochState(transactionState: TransactionState, epochCollectionNode: EpochCollectionNode) {

  lazy val epochNode: EpochNode =
    epochCollectionNode.getChild(transactionState.checkPointId)

}
