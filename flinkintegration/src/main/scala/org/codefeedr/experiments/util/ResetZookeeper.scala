package org.codefeedr.experiments.util
import org.codefeedr.experiments.{ExperimentBase, HotIssueQuery}

object ResetZookeeper extends ExperimentBase {

  def main(args: Array[String]): Unit = {
    initialize(args)
    logger.info("Clearing zookeeper storage")
    awaitReady(zkClient.deleteRecursive("/"))
    logger.info("Done clearing")
  }
}
