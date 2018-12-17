package org.codefeedr.experiments
import org.codefeedr.core.library.internal.SubjectTypeFactory
import org.codefeedr.core.library.internal.manager.SourceAlignment
import org.codefeedr.core.library.metastore.{QuerySourceNode, SubjectNode}
import org.codefeedr.experiments.model.HotIssue

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Main class to manage the alignment of a job
  */
object AlignmentController extends ExperimentBase {

  def main(args: Array[String]): Unit = {
    logger.info("Main class of alignment controller called")
    initialize(args)
    configurationProvider.initEc(getEnvironment.getConfig)
    logger.info("Alignment controller initialized")
    align()
  }

  private def align(): Unit = {
    val subjectName = configurationProvider.get("alignmentSubject")
    val sourceName = configurationProvider.get("alignmentSource")
    val subject = getAlignmentSubject(subjectName)
    val querySourceNode = getQuerySourceNode(subject, sourceName)
    val manager = new SourceAlignment(querySourceNode)
    logger.info("Starting alignment")
    awaitReady(manager.startAlignment())
    logger.info("Alignment started")
    if (Await.result(manager.whenReady(), 60.seconds)) {
      logger.info("Sources are ready for synchronization")
      manager.startRunningSynchronized()
      logger.info("Synchronization started")
      if (Await.result(manager.whenSynchronized(), 60.seconds)) {
        logger.info("Sources are synchronized")
      } else {
        throw new IllegalStateException("Failed to synchronize sources")
      }
    } else {
      throw new IllegalStateException("Failed prepare all sources")
    }
  }

  /**
    * Retrieves the subject on which to align.
    * Logs some error whenever the subject cannot be found
    * @return
    */
  private def getAlignmentSubject(subjectName: String): SubjectNode = {

    logger.info(s"Synchronizing on subject $subjectName")
    val subject = subjectLibrary.getSubject(subjectName)
    if (!awaitReady(subject.exists())) {
      val subjects = awaitReady(subjectLibrary.getSubjects().getNames()).mkString(",")
      logger.error(s"Cannot find a subject with the name $subjectName. Subjects are: $subjects")
    } else {
      logger.info(s"Subject $subjectName found. Starting alignment")
    }
    subject
  }

  /**
    * Finds the querysourcenode from a subject
    * TODO: Pass some reference to the job
    * @param subject
    * @return
    */
  private def getQuerySourceNode(subject: SubjectNode, querySourceName: String): QuerySourceNode = {
    val querySource = subject.getSources().getChild(querySourceName)
    if (awaitReady(querySource.exists())) {
      logger.info(s"Querysource $querySourceName was found. Starting alignment")
      querySource

    } else {
      val names = awaitReady(subject.getSourceNames())
      throw new IllegalStateException(
        s"Could not find querysource $querySourceName on subject ${subject.name}. Names are: ${names
          .mkString(",")}")
    }
  }

  /**
    * Starts the alignment on the passed subject
    * @param node subjectnode to align on
    */
  private def startAlignment(sourceNode: QuerySourceNode) = {
    val sourceAlignment = new SourceAlignment(sourceNode)
  }

}
