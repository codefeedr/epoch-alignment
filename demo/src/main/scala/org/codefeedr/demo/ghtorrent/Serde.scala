package org.codefeedr.demo.ghtorrent

import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Trait that should be implemented implicitly to make use of the Serde type class
  *
  * @tparam T Type to be (de)serialized
  */
trait Serde[T] {
  def serialize(o: T): String
  def deserialize(s: String): T
}

object Serde {
  def serialize[T: Serde](o: T): String = Serde[T].serialize(o)
  def deserialize[T: Serde](s: String): T = Serde[T].deserialize(s)

  def apply[T](implicit s: Serde[T]): Serde[T] = s
  /*

  implicit class SerOps[S:Serde](val a:S) extends AnyVal {
    def serialize:String =Serde[S].serialize(a)
  }

  implicit class DeserOps[S:Serde](val s:String) extends AnyVal {
    def deserialize:S = Serde[S].deserialize(s)
  }
 */
}
