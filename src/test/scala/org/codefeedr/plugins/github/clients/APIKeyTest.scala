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
package org.codefeedr.plugins.github.clients

import org.scalatest.FlatSpec

class APIKeyTest extends FlatSpec {

  "An API key" should "correctly initialized" in {
    val currentTime = System.currentTimeMillis()
    val key = new APIKey("key1", 10, currentTime)

    assert(key.getKey() == "key1")
    assert(key.getLimit() == 10)
    assert(key.getRequestsLeft() == 10)
    assert(key.getResetTime() == currentTime)
  }

  "An API key" should "be able to be reset" in {
    val currentTime = System.currentTimeMillis()
    val key = new APIKey("key2", 10, currentTime)

    key.updateRequestLeft(9)
    assert(key.getRequestsLeft() == 9)
    key.reset()
    assert(key.getRequestsLeft() == 10)
  }

  "An API key" should "be updated with a new time" in {
    val currentTime = System.currentTimeMillis()
    val key = new APIKey("key3", 10, currentTime)

    assert(key.getResetTime() == currentTime)

    val newTime = currentTime + 10
    key.updateResetTime(newTime)
    assert(key.getResetTime() == newTime)
  }

  "An API key" should "create the correct string representation" in {
    val currentTime = System.currentTimeMillis()
    val key = new APIKey("key4", 10, currentTime)

    assert(key.toString == s"APIKey(key=key4, requestLimit=10, requestsLeft=10, resetTime=$currentTime)")
  }




}
