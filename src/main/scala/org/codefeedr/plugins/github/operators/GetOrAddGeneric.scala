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

package org.codefeedr.plugins.github.operators

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.bson.conversions.Bson
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes._
import com.mongodb.client.model.IndexOptions
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.codefeedr.plugins.github.clients.MongoDB
import org.mongodb.scala.model.Filters._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import collection.JavaConverters._
import scala.async.Async._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Get or (retrieve and) add a value to MongoDB.
  * @tparam A the input type.
  * @tparam B the output type.
  */
abstract class GetOrAddGeneric[A: ClassTag, B: ClassTag]() extends RichAsyncFunction[A, B] {

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  //lazily load the logger
  private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  //mongodb instance
  @transient
  lazy val mongoDB: MongoDB = new MongoDB()

  /**
    * Called when runtime context is started.
    * @param parameters of this job.
    */
  override def open(parameters: Configuration): Unit = {
    setIndexes(getIndexNames)
  }

  /**
    * Sets the indexes.
    * @param indexes the indexes to set.
    */
  def setIndexes(indexes: Seq[String]) = {
    mongoDB
      .getCollection[B](getCollectionName)
      .createIndex(ascending(indexes: _*), new IndexOptions().unique(true))
      .toFuture()
  }

  /**
    * Invoked to do a asynchronous request.
    * @param input the input of type A.
    * @param resultFuture future of output type B.
    */
  override def asyncInvoke(input: A, resultFuture: ResultFuture[B]): Unit = {
    val col = mongoDB.getCollection[B](getCollectionName)

    //keep track if result is already send
    var send = false
    col
      .find(constructFilter(getIndexNames.zip(getIndexValues(input)).seq))
      .first()
      .subscribe(new Observer[B] {

        override def onError(e: Throwable): Unit = {
          logger.error(s"Error while finding document $input in MongoDB: ${e.getMessage}")
          resultFuture.complete(Iterable().asJavaCollection) //empty output
        }

        override def onComplete(): Unit = {
          if (send) return //if already send, then future is closed

          //retrieve output
          val getReturn: Option[B] = getFunction(input)

          //if some error occurred while retrieving, just return nothing
          if (getReturn.isEmpty) {
            logger.error(s"Request service didn't return commit for $input.")
            resultFuture.complete(Iterable().asJavaCollection)
          }

          //get commit
          val output = getReturn.get

          //insert commit and wait for it //TODO: Is this the way to go? Thread-safety...
          val result = Await.ready(col.insertOne(output).toFuture(), Duration.Inf)

          //match on await;
          //we want to filter duplicates once again, because they might have been processed at the same time
          result.value.get match {
            case Success(_) =>
              resultFuture.complete(Iterable(output).asJavaCollection) //if success then forward
            case Failure(_) =>
              resultFuture.complete(Iterable().asJavaCollection) //else empty forward
          }
        }

        override def onNext(result: B): Unit = {
          //data found, so we end with empty future (we don't want to forward duplicates).
          send = true
          resultFuture.complete(Iterable().asJavaCollection)
        }
      })
  }

  /**
    * Matches a sequence of key,values into an and/equal filter.
    * @param fieldsAndValues the fields and values to transform.
    * @return the final filter.
    */
  def constructFilter(fieldsAndValues: Seq[(String, String)]): Bson = fieldsAndValues match {
    case (a, b) :: Nil => equal(a, b) //if only 1 element, just return an equal
    case (a, b) :: tail => and(equal(a, b), constructFilter(tail)) //if still a tail, create an and
  }

  /**
    * Get the name of the collection to store in.
    * @return the name of the collection.
    */
  def getCollectionName: String

  /**
    * Get the name of the index.
    * @return the name of the index.
    */
  def getIndexNames: Seq[String]

  /**
    * Get the value of the index.
    * @param input to retrieve value from.
    * @return the value of the index.
    */
  def getIndexValues(input: A): Seq[String]

  /**
    * Factory method to retrieve B using A
    * @param input the input variable A.
    * @return the output variable B.
    */
  def getFunction(input: A): Option[B]

}
