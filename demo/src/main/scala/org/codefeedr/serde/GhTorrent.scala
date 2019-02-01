package org.codefeedr.serde

import org.codefeedr.Json4sSerde
import org.codefeedr.demo.ghtorrent.Serde
import org.codefeedr.ghtorrent.{Commit, Project, User}

/**
  * Import this object to have all json4s serdes available
  */
object GhTorrent {
  implicit val userSerde: Serde[User] = Json4sSerde[User]()
  implicit val commitSerde: Serde[Commit] = Json4sSerde[Commit]()
  implicit val projectSerde: Serde[Project] = Json4sSerde[Project]()
}
