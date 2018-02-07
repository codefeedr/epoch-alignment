package org.codefeedr.Model.zookeeper

/**
  * Case class describing the consumer of a partition of a subject
  * Stored in zookeeper
  *
  * @param uuid Identifies the consumer
  * @param partitions current subscribed partitions, and last read offsets
  * @param timeStamp updated every time the consumer is updated in zookeeper
  */
case class Consumer(uuid: String, partitions: List[Partition], timeStamp: Long)
