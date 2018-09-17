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

package org.codefeedr.ghtorrent

import java.util.Date

import org.joda.time.DateTime

/*
  Case classes representing the ghtorrent data
  For ghtorrent, see: http://ghtorrent.org/

  Based on: http://ghtorrent.org/files/schema.png

  Work in progress, classes are added as needed
 */

trait EventTime {
  val eventTime: DateTime
}

/**
  *
  * @param id
  * @param login
  * @param name
  * @param company
  * @param email
  * @param created_at
  * @param `type`
  * @param fake
  * @param deleted
  * @param long
  * @param lat
  * @param country_code
  * @param state
  * @param city
  */
case class User(id: Int,
                login: String,
                name: String,
                company: String,
                email: String,
                created_at: String,
                `type`: String,
                fake: Boolean = false,
                deleted: Boolean = false,
                long: Option[Float],
                lat: Option[Float],
                country_code: Option[String],
                state: Option[String],
                city: Option[String],
                updated_at: DateTime,
                eventTime: DateTime)
    extends EventTime

/**
  * Github commit
  * @param id
  * @param sha
  * @param author_id
  * @param committer_id
  * @param project_id
  * @param created_at
  */
case class Commit(id: Int,
                  sha: String,
                  author_id: Int,
                  committer_id: Int,
                  project_id: Int,
                  created_at: Date,
                  eventTime: DateTime)
    extends EventTime

/**
  * Project on github
  * @param id
  * @param url
  * @param owner_id
  * @param description
  * @param language
  * @param created_at
  * @param forked_from
  * @param deleted
  * @param updated_at
  */
case class Project(id: Int,
                   url: String,
                   owner_id: Int,
                   description: String,
                   language: String,
                   created_at: Date,
                   forked_from: Int,
                   deleted: Boolean,
                   updated_at: Date,
                   eventTime: DateTime)
    extends EventTime
