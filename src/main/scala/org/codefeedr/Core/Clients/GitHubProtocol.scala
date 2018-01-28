package org.codefeedr.Core.Clients

import java.util.Date

/**
  * Case classes related to the GitHubAPI.
  */
object GitHubProtocol {

  //simplistic view of a push event
  case class PushEvent(id: String,
                       repo_name: String,
                       ref: String,
                       beforeSHA: String,
                       afterSHA: String,
                       created_at: Date)

  //simplistic view of a commit
  case class Commit(url: String,
                    sha: String,
                    authorName: String,
                    authorEmail: String,
                    message: String,
                    comment_count: String,
                    tree: Tree,
                    parents: List[Parent])

  case class Parent(url: String, sha: String)

  case class Tree(url: String, sha: String)

}
