package org.codefeedr.core.library.metastore

import org.codefeedr.model.zookeeper.Partition

case class Epoch(epochIndex: Long, partitions: Iterable[Partition])
