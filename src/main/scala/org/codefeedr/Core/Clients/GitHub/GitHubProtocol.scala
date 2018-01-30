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
    * START '/events'
    */
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

  /**
    * END '/events'
    */
  /**
    * START '/repos/:repo/:name/commits/:sha'
    */
  /**
    * Represents all the information of a commit.
    * @param sha the sha of the commit.
    * @param commit the commit data.
    * @param author the author of the commit.
    * @param committer the commiter.
    * @param parents the parents of the commit.
    * @param stats the stats of the commit.
    * @param files the (diff) files of the commit.
    */
  case class Commit(sha: String,
                    commit: CommitData,
                    author: User,
                    committer: User,
                    parents: List[Parent],
                    stats: Stats,
                    files: List[File])

  /**
    * Represents the commit data specified in the commit.
    * @param author the author of the commmit.
    * @param committer the committer.
    * @param message the message of the commit.
    * @param tree the commit tree.
    * @param comment_count the comment count.
    * @param verification the verification object.
    */
  case class CommitData(author: CommitUser,
                        committer: CommitUser,
                        message: String,
                        tree: Tree,
                        comment_count: Int,
                        verification: Verification)

  /**
    * Represents the author/committer of the commit (data).
    * @param name name of the user.
    * @param email email of the user.
    * @param date date of the commit.
    */
  case class CommitUser(name: String, email: String, date: Date)

  /**
    * Represent the author/committer of the commit.
    * @param id the id of the user.
    * @param login the login of the user.
    * @param avatar_url the avatar url of the user.
    * @param `type` the user type.
    * @param site_admin if it is a site_admin or not.
    */
  case class User(id: Long, login: String, avatar_url: String, `type`: String, site_admin: Boolean)

  /**
    * Represents the verification of a commit.
    * @param verified if the commit was verified.
    * @param reason the verification reason.
    * @param signature the verification signature.
    * @param payload the verification payload.
    */
  case class Verification(verified: Boolean, reason: String, signature: String, payload: String)

  /**
    * Represents the stats of a commit
    * @param total total files edited.
    * @param additions total additions.
    * @param deletions total deletions.
    */
  case class Stats(total: Int, additions: Int, deletions: Int)

  /**
    * Represents the (diff) file in a commit.
    * @param sha sha of the diff.
    * @param fileName the name of the file.
    * @param additions the amount of additions.
    * @param deletions the amount of the deletions.
    * @param changes the amount of changes.
    * @param blob_url the url to the blob content.
    * @param raw_url the raw url.
    * @param contents_url the url of the contents.
    * @param patch the patch information.
    */
  case class File(sha: String,
                  fileName: String,
                  additions: Int,
                  deletions: Int,
                  changes: Int,
                  blob_url: String,
                  raw_url: String,
                  contents_url: String,
                  patch: String)

  /**
    * The parents of this commit.
    * @param sha the commit sha.
    */
  case class Parent(sha: String)

  /**
    * The tree of the commit.
    * @param sha the tree sha.
    */
  case class Tree(sha: String)

  /**
    * END '/repos/:repo/:name/commits/:sha'
    */
  /**
    * START '/repos/:repo/:name/commits'
    */
  /**
    * Represents a simplistic view of a commit.
    * @param sha the sha of a commit.
    */
  case class SimpleCommit(sha: String)

  /**
  * END '/repos/:repo/:name/commits'
  */

}
