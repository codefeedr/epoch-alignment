package org.codefeedr.demo.ghtorrent

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

  def apply[T](implicit s: Serde[T]): Serde[T] = s
  object ops {
    def serialize[T: Serde](o: T): String = Serde[T].serialize(o)

    def deserialize[T: Serde](s: String): T = Serde[T].deserialize(s)

    implicit class SerOps[S](val a: S) extends AnyVal {
      def serialize(implicit serde: Serde[S]): String = serde.serialize(a)
    }

    implicit class DeserOps[S](val s: String) extends AnyVal {
      def deserialize(implicit serde: Serde[S]): S = serde.deserialize(s)
    }

  }
}
