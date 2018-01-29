package org.codefeedr.Core.Clients

import java.util.Date

/**
  * Case classes related to the GitHubAPI.
  */
object GitHubProtocol {

  //Represents a PushEvent (including payload) simplified.
  case class PushEvent(id: String,
                       //pushId : String, MIGHT WANT TO ADD THIS LATER, CURRENTLY NOT SUPPORTED BY LIBRARY
                       repo: Repo,
                       actor: Actor,
                       ref: String,
                       size: Int,
                       head: String,
                       before: String,
                       commits: List[CommitSimple],
                       created_at: Date)

  //Represents a repository
  case class Repo(id: Long, name: String, url: String)

  //Represents an actor
  case class Actor(id: Long, login: String, url: String)

  //Represents a Commit embedded in a PushEvent
  case class CommitSimple(sha: String, author: UserSimple, message: String)

  //Represent the author of a Commit
  case class UserSimple(email: String, name: String)

  //Represents a commit
  case class Commit(sha: String,
                    repo: Repo,
                    author: User,
                    committer: User,
                    message: String,
                    comment_count: Int,
                    tree: Tree,
                    verification: Verification,
                    parents: List[Parent],
                    stats: Stats,
                    files: List[File])

  // Represent the committer/author of a commit
  case class User(id: Long, name: String, emai: String, login: String, `type`: String, date: Date)

  // Represents the verification of a commit
  case class Verification(verified: Boolean, reason: String, signature: String, payload: String)

  //Represent the stats of a commit
  case class Stats(total: Integer, additions: Integer, deletions: Integer)

  //Represents the changed file of a commit
  case class File(sha: String,
                  fileName: String,
                  additions: Integer,
                  deletions: Integer,
                  changes: Integer,
                  blob_url: String,
                  raw_url: String,
                  contents_ur: String,
                  patch: String)

  //Represents the parent of a commit
  case class Parent(sha: String, url: String, html_url: String)

  //Represent the tree of a commit
  case class Tree(url: String, sha: String)

}
