package org.codefeedr.Core.Clients

import com.typesafe.config.{Config, ConfigFactory}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.codefeedr.Core.Plugin.PushEvent
import org.mongodb.scala.{
  MongoClient,
  MongoClientSettings,
  MongoCredential,
  MongoDatabase,
  ServerAddress
}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.connection.ClusterSettings

import scala.collection.JavaConverters._

/**
  * Wrapper class for setting up MongoDB connection.
  * Currently it only supports a PushEvent
  * TODO: Fix this ^^
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
    .codecRegistry(fromRegistries(fromProviders(classOf[PushEvent]), DEFAULT_CODEC_REGISTRY))
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
}
