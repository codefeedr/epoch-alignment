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
package org.codefeedr.core.library.internal

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{ConfigConstants, Configuration, WebOptions}
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.library.SubjectFactory
import org.codefeedr.core.library.internal.kafka.source.{KafkaGenericSource, KafkaRowSource}
import org.codefeedr.core.library.metastore.SubjectLibrary
import org.codefeedr.model.SubjectType

import scala.concurrent._
import ExecutionContext.Implicits.global
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import async.Async.{async, await}
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class Job[Input: ru.TypeTag: ClassTag: TypeInformation, Output: ru.TypeTag: ClassTag](
    name: String) {

  //logger
  lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName))

  //job subjecttype
  var subjectType: SubjectType = _

  //custom source of a job
  var source: RichSourceFunction[Input] = _

  /**
    * Returns the amount of parallel workers.
    * @return by default 1
    */
  def getParallelism: Int = 1

  /**
    * Setups a stream for the given environment.
    * @param env the environment to setup the stream on.
    * @return the prepared datastream.
    */
  def getStream(env: StreamExecutionEnvironment): DataStream[Output]

  /**
    * Composes the source on the given environment.
    * Registers all meta-information.
    * @param env the environment where the source should be composed on.
    */
  def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
    val sinkName = s"composedsink_${queryId}"
    val sink = await(SubjectFactory.GetSink[Output](sinkName))
    val stream = getStream(env)
    stream.addSink(sink)
  }

  /**
    * Setup the subjecttype.
    * @param subjectLibrary the subject library.
    */
  def setupType(subjectLibrary: SubjectLibrary) = async {
    subjectType = await(subjectLibrary.getSubject[Output]().getOrCreateType[Output]())
  }

  /**
    * Creates a generic source based on a Job.
    * @param job the job from which the source is composed.
    */
  def setSource(job: Job[_, Input]) = {
    source = new KafkaGenericSource[Input](job.subjectType, job.subjectType.uuid)
  }

  /**
    * Starts a job asynchronous.
    */
  def startJob() = async {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironment(getParallelism, conf)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, Time.of(1, TimeUnit.SECONDS)))
    logger.debug(s"Composing env for ${subjectType.name}")
    await(compose(env, s"$name"))
    logger.debug(s"Starting env for ${subjectType.name}")
    env.execute()
    logger.debug(s"Completed env for ${subjectType.name}")
  }

}
