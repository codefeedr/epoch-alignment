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
import org.json4s.JObject

/**
  * Case classes related to the GitHubAPI.
  */
object GitHubProtocol {

  /**
    * Represents a 'generic' GitHub event.
    * @param id unique identifier of the event.
    * @param repo the repository of the event.
    * @param `type` the type of event.
    * @param actor the actor of the event.
    * @param org organization of the event (optional).
    * @param payload the payload of the event (different per type).
    * @param public if event is public.
    * @param created_at date at which event is published.
    */
  case class Event(id: String,
                   repo: Repo,
                   `type`: String,
                   actor: Actor,
                   org: Option[Organization],
                   payload: JObject,
                   public: Boolean,
                   created_at: Date)

  /**
    * Represents a 'generic' GitHub event.
    * @param id unique identifier of the event.
    * @param repo the repository of the event.
    * @param actor the actor of the event.
    * @param org organization of the event (optional).
    * @param payload the payload of the event.
    * @param public if event is public.
    * @param created_at date at which event is published.
    */
  case class PushEvent(id: String,
                       repo: Repo,
                       actor: Actor,
                       org: Option[Organization],
                       payload: Payload,
                       public: Boolean,
                       created_at: Date)

  /**
    * Payload of the PushEvent.
    * @param push_id id of the push.
    * @param size size of push.
    * @param distinct_size distinct size of the push.
    * @param ref ref of the push.
    * @param head head SHA after the push.
    * @param before before SHA.
    * @param commits commits of the push.
    */
  case class Payload(push_id: Long,
                     size: Int,
                     distinct_size: Int,
                     ref: String,
                     head: String,
                     before: String,
                     commits: List[PushCommit])

  /**
    * Represents the organization of an event.
    * @param id the id of the organization.
    * @param login the login of the organization.
    */
  case class Organization(id: Long, login: String)

  /**
    * Represents the repository of an event.
    * @param id the id of the repository.
    * @param name the name of the repository.
    */
  case class Repo(id: Long, name: String)

  /**
    * Represents the actor of an event.
    * @param id the id of the actor.
    * @param login the login of the actor.
    * @param display_login the display login of the actor.
    * @param avatar_url url of the avatar of the actor.
    */
  case class Actor(id: Long, login: String, display_login: String, avatar_url: String)

  /**
    * Represents a commit embedded in a PushEvent
    * @param sha the sha of the commit.
    * @param author the author of the commit.
    * @param message the message of the commit.
    * @param distinct true if the commit is distinct.
    */
  case class PushCommit(sha: String, author: PushAuthor, message: String, distinct: Boolean)

  /**
    * Represent the author of a PushCommit.
    * @param email the email of the author.
    * @param name the name of the author.
    */
  case class PushAuthor(email: String, name: String)

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
