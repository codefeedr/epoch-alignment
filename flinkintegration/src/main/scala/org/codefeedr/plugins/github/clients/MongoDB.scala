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

import com.typesafe.config.{Config, ConfigFactory}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.codefeedr.plugins.github.clients.GitHubProtocol._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.connection.ClusterSettings
import org.mongodb.scala.{
  MongoClient,
  MongoClientSettings,
  MongoCollection,
  MongoCredential,
  MongoDatabase,
  ServerAddress
}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Wrapper class for setting up MongoDB connection.
  * Currently supports Commit and PushEvent (try to make it generic)
  */
class MongoDB {

  //get the codefeedr configuration files
  private lazy val conf: Config = ConfigFactory.load()

  //setup credentials from config
  @transient
  private lazy val mongoCredential: MongoCredential = MongoCredential.createCredential(
    conf.getString("codefeedr.mongo.username"),
    conf.getString("codefeedr.mongo.db"),
    conf.getString("codefeedr.mongo.password").toCharArray
  )

  //setup server address from config
  @transient
  private lazy val mongoServer: ServerAddress = new ServerAddress(
    conf.getString("codefeedr.mongo.host"),
    conf.getInt("codefeedr.mongo.port")
  )

  //set all settings
  @transient
  private lazy val mongoSettings: MongoClientSettings = MongoClientSettings
    .builder()
    .clusterSettings(ClusterSettings.builder().hosts(List(mongoServer).asJava).build())
    .codecRegistry(fromRegistries(
      fromProviders(
        classOf[PushEvent],
        classOf[Organization],
        classOf[Payload],
        classOf[Repo],
        classOf[Actor],
        classOf[PushAuthor],
        classOf[PushCommit],
        classOf[Commit],
        classOf[CommitData],
        classOf[CommitUser],
        classOf[User],
        classOf[Verification],
        classOf[Stats],
        classOf[File],
        classOf[Parent],
        classOf[Tree]
      ),
      DEFAULT_CODEC_REGISTRY
    ))
    //.credentialList(List(mongoCredential).asJava)
    .build()

  //setup client
  @transient
  private lazy val _mongoClient: MongoClient = MongoClient(mongoSettings)

  //setup correct database and register codec
  @transient
  private lazy val _mongoDatabase: MongoDatabase =
    mongoClient.getDatabase(conf.getString("codefeedr.mongo.db"))

  //some getters
  def mongoClient = _mongoClient
  def mongoDatabase = _mongoDatabase

  /**
    * Get the correct collection based on the type.
    * @param collectionName the name of the collection.
    * @tparam T the type of the collection that you want
    * @return the correct collection, based on type.
    */
  def getCollection[T: ClassTag](collectionName: String): MongoCollection[T] = {
    mongoDatabase.getCollection[T](collectionName)
  }
}
