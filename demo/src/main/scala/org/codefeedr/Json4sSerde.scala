package org.codefeedr

import org.codefeedr.demo.ghtorrent.Serde
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class Json4sSerde[T <: AnyRef: ClassTag: ru.TypeTag](val f: Formats) extends Serde[T] {
  implicit val formats: Formats = f

  override def serialize(o: T): String = write[T](o)

  override def deserialize(s: String): T = parse(s).extract[T]
}

object Json4sSerde {
  def apply[T <: AnyRef: ClassTag: ru.TypeTag](): Json4sSerde[T] =
    new Json4sSerde[T](DefaultFormats)

  def apply[T <: AnyRef: ClassTag: ru.TypeTag](formats: Formats): Json4sSerde[T] =
    new Json4sSerde[T](formats)
}
