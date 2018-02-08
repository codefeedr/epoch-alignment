package org.codefeedr.model.zookeeper

/**
  * Case class describing a sink of a query.
  * Most data is located in the child nodes of each individual Producer
  * @param uuid
  */
case class QuerySink(uuid: String)
