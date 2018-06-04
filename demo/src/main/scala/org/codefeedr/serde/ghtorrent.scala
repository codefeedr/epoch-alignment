package org.codefeedr.serde

import org.codefeedr.Json4sSerde
import org.codefeedr.demo.ghtorrent.Serde
import org.codefeedr.ghtorrent.User
import org.json4s.{DefaultFormats, FieldSerializer}
import org.json4s.FieldSerializer._

/**
  * Import this object to have all json4s serdes available
  */
object ghtorrent {
  implicit val userSerde: Serde[User] = Json4sSerde[User]
}
