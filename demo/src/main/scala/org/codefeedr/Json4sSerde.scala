package org.codefeedr

import org.codefeedr.demo.ghtorrent.Serde
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.ext.{JavaTypesSerializers, JodaTimeSerializers}
import org.json4s.native.Serialization.write

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class Json4sSerde[T <: AnyRef: ClassTag: ru.TypeTag](val f: Option[Formats] = None)
    extends Serde[T] {
  implicit val formats: Formats =
    f.getOrElse(MyJsonFormats.isoDateTimeFormats)

  override def serialize(o: T): String = write[T](o)

  override def deserialize(s: String): T = parse(s).extract[T]
}

object Json4sSerde {
  def apply[T <: AnyRef: ClassTag: ru.TypeTag](): Json4sSerde[T] =
    new Json4sSerde[T](None)

  def apply[T <: AnyRef: ClassTag: ru.TypeTag](formats: Formats): Json4sSerde[T] =
    new Json4sSerde[T](Some(formats))
}

object MyJsonFormats {

  DateTimeZone.setDefault(DateTimeZone.UTC)

  class IsoDateTimeFormats(fieldName: String = "someDateTimeFieldName")
      extends Serializer[DateTime] {
    private val DateClass = classOf[DateTime]

    private val df = ISODateTimeFormat.dateTime()
    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), DateTime] = {
      case (TypeInfo(DateClass, _), json) =>
        json match {

          case JObject(JField(`fieldName`, JString(s)) :: Nil) =>
            df.parseDateTime(s).withZone(DateTimeZone.UTC)

          case JString(s) =>
            df.parseDateTime(s).withZone(DateTimeZone.UTC)

          case x: Any =>
            throw new MappingException(s"Can't convert $x to DateTime")
        }
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case d: DateTime => JObject(JField(fieldName, JString(d.toString)) :: Nil)
    }
  }

  implicit lazy val formats = DefaultFormats ++
    JodaTimeSerializers.all ++
    JavaTypesSerializers.all

  lazy val isoDateTimeFormats = formats + new IsoDateTimeFormats()
}
