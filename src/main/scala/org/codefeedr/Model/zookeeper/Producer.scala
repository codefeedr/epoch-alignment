package org.codefeedr.Model.zookeeper

/**
  * Case class describing a producers state
  *
  * @param uuid identifies the producer
  * @param partitions Last known offsets when the producer updated its state
  * @param timestamp Moment of last update
  */
case class Producer(uuid: String, partitions: List[Partition], timestamp: Long)
