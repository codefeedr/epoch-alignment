

package org.codefeedr.Core.Library

import java.util.UUID

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.Core.Library.Internal.Kafka.{KafkaController, KafkaGenericSink, KafkaSource}
import org.codefeedr.Core.Library.Internal.{KeyFactory, RecordTransformer}
import org.codefeedr.Model.{ActionType, SubjectType, TrailedRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * ThreadSafe
  * Created by Niels on 18/07/2017.
  */
object SubjectFactory {
  def GetSink[TData: ru.TypeTag: ClassTag]: Future[SinkFunction[TData]] = {
    SubjectLibrary
      .GetOrCreateType[TData]()
      .flatMap(o =>
        KafkaController.GuaranteeTopic(s"${o.name}_${o.uuid}").map(_ => new KafkaGenericSink(o)))
  }

  /**
    * Construct a serializable and distributable mapper function from any source type to a TrailedRecord
    * @tparam TData Type of the source object to get a mapper for
    * @return A function that can convert the object into a trailed record
    */
  def GetTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TData => TrailedRecord = {
    @transient lazy val transformer = new RecordTransformer[TData](subjectType)
    @transient lazy val keyFactory = new KeyFactory(subjectType, UUID.randomUUID())
    (d: TData) =>
      {
        val record = transformer.Bag(d, ActionType.Add)
        val trail = keyFactory.GetKey(record)
        TrailedRecord(record, trail)
      }
  }

  /**
    * Construct a deserializer to transform a TrailedRecord back into some object
    * Meant to use for development & testing, not very safe
    * @param subjectType The type of the record expected
    * @tparam TData Type of the object to transform to
    * @return The object
    */
  def GetUnTransformer[TData: ru.TypeTag: ClassTag](
      subjectType: SubjectType): TrailedRecord => TData = {
    @transient lazy val transformer = new RecordTransformer[TData](subjectType)
    (r: TrailedRecord) =>
      transformer.Unbag(r.record)
  }

  def GetSource(subjectType: SubjectType): SourceFunction[TrailedRecord] = {
    new KafkaSource(subjectType)
  }
}
