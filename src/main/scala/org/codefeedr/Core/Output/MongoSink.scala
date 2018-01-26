package org.codefeedr.Core.Output

import com.mongodb.client.model.IndexOptions
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.mongodb.scala._
import org.mongodb.scala.connection.ClusterSettings

import scala.async.Async.{async, await}
import scala.collection.JavaConverters._
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

class MongoSink[A](collectionName: String, indexes: String*) extends RichSinkFunction[A] {

  //register codec for this sink
  val codecRegistry = fromRegistries(fromProviders(classOf[A]), DEFAULT_CODEC_REGISTRY)

  //get the codefeedr configuration files
  lazy val conf: Config = ConfigFactory.load()

  //setup credentials from config
  @transient
  lazy val mongoCredential: MongoCredential = MongoCredential.createCredential(
    conf.getString("codefeedr.mongo.username"),
    conf.getString("codefeedr.mongo.db"),
    conf.getString("codefeedr.mongo.password").toCharArray
  )

  //setup server address from config
  @transient
  lazy val mongoServer: ServerAddress = new ServerAddress(
    conf.getString("codefeedr.mongo.host"),
    conf.getInt("codefeedr.mongo.port")
  )

  //set all settings
  @transient
  lazy val mongoSettings: MongoClientSettings = MongoClientSettings
    .builder()
    .clusterSettings(ClusterSettings.builder().hosts(List(mongoServer).asJava).build())
    .credentialList(List(mongoCredential).asJava)
    .build()

  //setup client
  @transient
  lazy val mongoClient: MongoClient = MongoClient(mongoSettings)

  //setup correct database and register codec
  @transient
  lazy val mongoDatabase: MongoDatabase =
    mongoClient.getDatabase(conf.getString("codefeedr.mongo.db")).withCodecRegistry(codecRegistry)

  /**
    * Invoked by Flink and inserts into Mongo.
    * @param value the event to store.
    */
  override def invoke(value: A): Unit = {
    async { //stores async
      mongoDatabase.getCollection[A](collectionName).insertOne(value)
    }
  }

  /**
    * Called when sink is opened and prepares mongo collection.
    * @param parameters the Flink configuration parameters.
    */
  override def open(parameters: Configuration): Unit = {
    val collections = await(mongoDatabase.listCollectionNames().toFuture())

    if (!collections.contains(collectionName)) {
      //create collection if it doesn't exist yet
      await(mongoDatabase.createCollection(collectionName).toFuture())
    }

    //set all the indexes
    setIndexes()

  }

  /**
    * Setup the (unique) indexes for this 'event'.
    */
  def setIndexes(): Unit = {
    mongoDatabase
      .getCollection(collectionName)
      .createIndex(ascending(indexes: _*), new IndexOptions().unique(true))

  }
}
