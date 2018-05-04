package org.codefeedr.core.library.metastore

/** Case class definiting a mapping from an epoch of a job to an epoch of a source */
case class EpochMapping(subject: String, epoch: Long)