package org.codefeedr.Library

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.Library.Internal.KafkaConsumerFactory
import org.codefeedr.Model.{Record, RecordIdentifier, SubjectType}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaSource[TData: TypeInformation](subjectType: SubjectType)
    extends RichSourceFunction[TData]
    with ResultTypeQueryable[TData]
    with LazyLogging {
  @transient private lazy val dataConsumer = {
    val consumer = KafkaConsumerFactory.create[RecordIdentifier, Record]()
    consumer.subscribe(Iterable(topic).asJavaCollection)
    logger.debug(s"Source $uuid subsribed on topic $topic")
    consumer
  }
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"
  @transient private var running = false
  @transient private lazy val uuid = UUID.randomUUID()

  //Make this configurable?
  @transient private val SubjectAwaitTime = 1000
  @transient private val RefreshTime = 100
  @transient private val PollTimeout = 100

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[TData]): Unit = {
    new Thread(new KafkaPoller(ctx)).run()
  }

  class KafkaPoller(ctx: SourceFunction.SourceContext[TData]) extends Runnable {
    def run(): Unit = {
      running = true
      while (running) {
        dataConsumer
          .poll(PollTimeout)
          .iterator()
          .asScala
          .map(o => o.value().asInstanceOf[TData])
          .foreach(ctx.collect)
        Thread.sleep(RefreshTime)
      }
    }
  }

  override def getProducedType: TypeInformation[TData] = createTypeInformation[TData]
}
