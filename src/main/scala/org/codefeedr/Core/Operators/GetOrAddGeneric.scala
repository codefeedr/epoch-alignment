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

package org.codefeedr.core.operators

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.bson.conversions.Bson
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes._
import com.mongodb.client.model.IndexOptions
import org.codefeedr.core.clients.MongoDB.MongoDB
import org.mongodb.scala.model.Filters._

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import collection.JavaConverters._
import scala.async.Async

/**
  * Get or (retrieve and) add a value to MongoDB.
  * @tparam A the input type.
  * @tparam B the output type.
  */
abstract class GetOrAddGeneric[A: ClassTag, B: ClassTag]() extends RichAsyncFunction[A, B] {

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  //mongodb instance
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
          resultFuture.complete(Iterable().asJavaCollection)
        }

        override def onComplete(): Unit = {
          if (send) { //if already send then ignore
            return
          }

          Async.async {
            //retrieve output
            val output: B = Async.await(getFunction(input))

            //insert and complete future
            col.insertOne(output).toFuture()
            resultFuture.complete(Iterable(output).asJavaCollection)
          }
        }

        override def onNext(result: B): Unit = {
          //found the record, so lets end the future
          send = true
          resultFuture.complete(Iterable(result).asJavaCollection)
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
  def getFunction(input: A): Future[B]
}
