/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.codefeedr.Core.Clients.GitHub

import java.util.Date

import com.google.gson.JsonElement

/**
  * Case classes related to the GitHubAPI.
  */
object GitHubProtocol {

  //Represents a default event.
  case class Event(id: String,
                   repo: Repo,
                   `type`: String,
                   actor: Actor,
                   payload: JsonElement,
                   public: Boolean,
                   created_at: Date)

  //Represents a PushEvent.
  case class PushEvent(id: String,
                       repo: Repo,
                       actor: Actor,
                       payload: Payload,
                       public: Boolean,
                       created_at: Date)

  //Represents the Payload of a PushEvent
  case class Payload(push_id: Long,
                     size: Int,
                     distinct_size: Int,
                     ref: String,
                     head: String,
                     before: String,
                     commits: java.util.List[CommitSimple])

  //Represent the organization
  case class Organization(id: Long, login: String, url: String)
  //Represents a repository
  case class Repo(id: Long, name: String, url: String)

  //Represents an actor
  case class Actor(id: Long, login: String, url: String)

  //Represents a Commit embedded in a PushEvent
  case class CommitSimple(sha: String, author: UserSimple, message: String, distinct: Boolean)

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
                    parents: java.util.List[Parent],
                    stats: Stats,
                    files: java.util.List[File])

  // Represent the committer/author of a commit
  case class User(id: Long, name: String, emai: String, login: String, `type`: String, date: Date)

  // Represents the verification of a commit
  case class Verification(verified: Boolean, reason: String, signature: String, payload: String)

  //Represent the stats of a commit
  case class Stats(total: Int, additions: Int, deletions: Int)

  //Represents the changed file of a commit
  case class File(sha: String,
                  fileName: String,
                  additions: Int,
                  deletions: Int,
                  changes: Int,
                  blob_url: String,
                  raw_url: String,
                  contents_ur: String,
                  patch: String)

  //Represents the parent of a commit
  case class Parent(sha: String, url: String, html_url: String)

  //Represent the tree of a commit
  case class Tree(url: String, sha: String)

}
