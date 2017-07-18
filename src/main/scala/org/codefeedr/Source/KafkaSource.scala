package org.codefeedr.Source

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.Library.Internal.{KafkaConsumerFactory, KafkaProducerFactory}
import org.codefeedr.Model.{Record, RecordIdentifier, SubjectType}

import scala.reflect.runtime.{universe => ru}
import scala.collection.JavaConverters._

/**
  * Created by Niels on 18/07/2017.
  */
class KafkaSource[TData: ru.TypeTag](subjectType: SubjectType) extends RichSourceFunction[TData] {
  @transient private lazy val dataConsumer = KafkaConsumerFactory.create[RecordIdentifier, Record]
  @transient private lazy val topic = s"${subjectType.name}_${subjectType.uuid}"

  @transient private var running = false

  //Make this configurable?
  @transient private val SubjectAwaitTime = 1000
  @transient private val RefreshTime = 1000
  @transient private val PollTimeout = 1000

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

}
