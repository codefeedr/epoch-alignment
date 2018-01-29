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

package org.codefeedr.Core.Output

import com.mongodb.client.model.IndexOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes._
import org.codefeedr.Core.Clients.MongoDB.{CollectionType, MongoDB}
import scala.reflect.ClassTag

/**
  * Setups a MongoSink on a generic type.
  * @param collectionType the type/name of the collection to store in
  * @param indexes the unique index(es) to create
  */
class MongoSink[T: ClassTag](collectionType: CollectionType, indexes: String*)
    extends RichSinkFunction[T] {

  //retrieve a connection to mongodb
  lazy val db: MongoDB = new MongoDB()

  /**
    * Invoked by Flink and inserts into Mongo.
    * @param value the event to store.
    */
  override def invoke(value: T): Unit = {
    db.getCollection[T](collectionType).insertOne(value).toFuture()
  }

  /**
    * Called when sink is opened and prepares mongo collection.
    * @param parameters the Flink configuration parameters.
    */
  override def open(parameters: Configuration): Unit = {
    setIndexes()
  }

  /**
    * Setup the (unique) indexes for this 'event'.
    */
  def setIndexes(): Unit = {
    db.getCollection(collectionType)
      .createIndex(ascending(indexes: _*), new IndexOptions().unique(true))
      .toFuture()
  }
}
