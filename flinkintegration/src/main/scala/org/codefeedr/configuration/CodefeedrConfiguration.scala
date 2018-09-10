package org.codefeedr.configuration

trait CodefeedrConfigurationComponent { this: ConfigurationProviderComponent =>

  /*
   * Make sure to use lazy here, because configuration needs to be initialized
   * Should find a different solution for that
   *  */
  lazy val codefeedrConfiguration: CodefeedrConfiguration = CodefeedrConfiguration(
    partitionCount = configurationProvider.getInt("codefeedr.kafka.partition.count"),
    producerCount = configurationProvider.getInt("codefeedr.kafka.producer.count")
  )
}

/**
  *
  * @param partitionCount Number of partitions used when creating kafka topics
  *                       Each source in Flink can expose data from one or more partitions,
  *                       so keep flink's parallism dividable by this number
  * @param producerCount  The amount of producers each instance of each kafka sink should maintain
  *                       For each uncommitted checkpoint each instance of each kafka sink maintains a producer
  *                       5 is the same used in flink's native kafka source implementation
  */
case class CodefeedrConfiguration(
    partitionCount: Int = 4,
    producerCount: Int = 5
)
