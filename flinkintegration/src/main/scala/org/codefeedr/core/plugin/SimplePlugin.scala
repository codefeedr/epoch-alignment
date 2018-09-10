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

package org.codefeedr.core.plugin

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.library.SubjectFactoryComponent
import org.codefeedr.core.library.internal.{AbstractPlugin, SubjectTypeFactory}
import org.codefeedr.model.SubjectType

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait SimplePluginComponent {
  this:SubjectFactoryComponent =>


  /**
    * Implement this class to expose a simple plugin
    * Created by Niels on 04/08/2017.
    */
  abstract class SimplePlugin[TData: ru.TypeTag: ClassTag](useTrailedSink: Boolean = false)
    extends AbstractPlugin
      with LazyLogging {


    /**
      * Method to implement as plugin to expose a datastream
      * Make sure this implementation is serializable!
      * @param env The environment to create the datastream on
      * @return The datastream itself
      */
    def getStream(env: StreamExecutionEnvironment): DataStream[TData]

    override def createSubjectType(): SubjectType = {
      SubjectTypeFactory.getSubjectType[TData]
    }

    /**
      * Composes the source on the given environment
      * Registers all meta-information
      * @param env the environment where the source should be composed on
      * @return
      */
    override def compose(env: StreamExecutionEnvironment, queryId: String): Future[Unit] = async {
      val sinkName = s"composedsink_${queryId}"
      //HACK: Direct call to libraryServices
      val sink = if (useTrailedSink) {
        await(subjectFactory.getGenericTrailedSink[TData](sinkName, queryId))
      } else {
        await(subjectFactory.getSink[TData](sinkName, queryId))
      }

      logger.debug(s"Got sink. Creating stream")
      val stream = getStream(env)
      stream.addSink(sink)
    }

    /**
      * Creates the type of the plugin as a subject.
      * Make sure to call this method only once!
      * @return a future when the call has completed
      */
    def createSubject(): Future[Unit] =
      subjectFactory.create[TData]().map(_ => ())

    /**
      * Deletes an existing registration of the subject, and creates a new one
      * Make sure to only call this when knowing for sure the subject is not in use
      * Otherwise this might result in unexpected behavior
      * @return A future that completes when the operation is done
      */
    def reCreateSubject(): Future[Unit] =
      subjectFactory.reCreate[TData]().map(_ => ())

  }


}

