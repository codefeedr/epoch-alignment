package org.codefeedr.experiments
import org.codefeedr.core.library.metastore.SubjectNode

/**
  * Main class to manage the alignment of a job
  */
object AlignmentController extends ExperimentBase {

  def main(args: Array[String]): Unit = {
    logger.info("Main class of alignment controller called")
    initialize(args)
    logger.info("Alignment controller initialized")
    align()

  }

  private def align(): Unit = {
    val subject = getAlignmentSubject()
  }

  /**
    * Retrieves the subject on which to align.
    * Logs some error whenever the subject cannot be found
    * @return
    */
  private def getAlignmentSubject(): SubjectNode = {
    val alignmentSubject = configurationProvider.get("alignmentSubject")
    logger.info(s"Synchronizing on subject $alignmentSubject")
    val subject = subjectLibrary.getSubject(alignmentSubject)
    if (!awaitReady(subject.exists())) {
      logger.info(s"Subject $alignmentSubject found. Starting alignment")
      val subjects = awaitReady(subjectLibrary.getSubjects().getNames()).mkString(",")
      logger.error(
        s"Cannot find a subject with the name $alignmentSubject. Subjects are: $subjects")
    } else {
      logger.error(s"Subject $alignmentSubject not found")
    }
    subject
  }

}
