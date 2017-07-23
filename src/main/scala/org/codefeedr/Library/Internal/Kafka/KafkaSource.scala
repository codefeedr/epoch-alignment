package org.codefeedr.Library.Internal.Kafka

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.codefeedr.Model.{Record, RecordIdentifier, SubjectType}

import scala.collection.JavaConverters._

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaSource[TData: TypeInformation](subjectType: SubjectType)
    extends RichSourceFunction[TData]
    with ResultTypeQueryable[TData]
    with LazyLogging {

  @transient private lazy val dataConsumer = {
    val consumer = KafkaConsumerFactory.create[RecordIdentifier, Record](uuid.toString)
    consumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(s"Source $uuid subsribed on topic $topic as group")
    consumer
  }
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"

  @transient private lazy val uuid = UUID.randomUUID()

  //Make this configurable?
  @transient private lazy val RefreshTime = 100
  @transient private lazy val PollTimeout = 1000

  @transient private var running = false

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[TData]): Unit = {
    running = true
    while (running) {
      dataConsumer
        .poll(PollTimeout)
        .iterator()
        .asScala
        .map(o => {
          o.value().data.asInstanceOf[TData]
        })
        .foreach(ctx.collect)
      Thread.sleep(RefreshTime)
    }
  }

  override def getProducedType: TypeInformation[TData] = createTypeInformation[TData]
}
