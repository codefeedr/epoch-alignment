package org.codefeedr.Model.Zookeeper

/**
  * Node in zookeeper describing a single query source.
  * Child nodes are the collection of consumers under the query source
  * Most information will be located in the consumer
  * @param uuid
  */
case class QuerySource(uuid: String)
