package org.codefeedr.Core.input



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

import org.codefeedr.Core.input.GithubConnector
import org.scalatest._

class GitHubConnectorTest extends AsyncFlatSpec with Matchers {

  "The GitHub connector" should "connect to GitHub" in {
    val gh = new GithubConnector
    val conn = gh.connectToGitHub()

    conn should not be null
    conn.getRemainingRequests shouldBe an[Integer]
  }

  "The GitHub connector" should "connect to the database" in {

    val mongo = new GithubConnector().connectToMongo()
    mongo should not be null
    mongo.count.head.map(r => assert(r >= 0))
  }
}
