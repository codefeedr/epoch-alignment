package org.codefeedr

import org.codefeedr.demo.ghtorrent.Serde

class Json4sSerde[T] extends Serde[T] {
  override def serialize(o: T): String = ???

  override def deserialize(s: String): T = ???
}
