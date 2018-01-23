package org.codefeedr.Model.Zookeeper

/**
  * Case class describing a producers state
  *
  * @param uuid identifies the producer
  * @param active Indicates whether the producer is still actively producing data
  * @param partitions Last known offsets when the producer updated its state
  * @param timestamp Moment of last update
  */
case class Producer(uuid: String, active: Boolean, partitions: List[Partition], timestamp: Long)
