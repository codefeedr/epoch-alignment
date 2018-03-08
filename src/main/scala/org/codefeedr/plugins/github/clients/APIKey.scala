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

/**
  * Corresponds to an API key.
  * @param key the actual key.
  * @param requestLimit the request limit.
  * @param resetTime the time on which a key is resetted (milliseconds since UNIX epoch).
  */
class APIKey(val key: String,
             val requestLimit : Int,
             var resetTime: Long) {

  /**
    * Keep track of the amount of request left.
    */
  var requestsLeft = requestLimit

  /**
    * Get the API key.
    * @return the api key.
    */
  def getKey() : String = key

  /**
    * Get the limit of the API key.
    * @return the rate limit of an key.
    */
  def getLimit() : Int = requestLimit

  /**
    * Get the amount of requests left.
    * @return the requests left.
    */
  def getRequestsLeft() : Int = requestsLeft

  /**
    * Get the time on which a rate limit is resetted.
    * @return the time on which the key is resetted (milliseconds since UNIX epoch).
    */
  def getResetTime() : Long = resetTime


  /**
    * Update the amount of requests left.
    * @param newRequestsLeft the new amount of requests left.
    */
  def updateRequestLeft(newRequestsLeft : Int) = requestsLeft = newRequestsLeft

  /**
    * Update the time a key is reset.
    * @param newResetTime the new reset time (milliseconds since UNIX epoch).
    */
  def updateResetTime(newResetTime : Long) = resetTime = newResetTime

  /**
    * Reset a key to its requestlimit.
    */
  def reset() = requestsLeft = requestLimit

}
