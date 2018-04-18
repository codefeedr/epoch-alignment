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

package org.codefeedr.core.engine.query

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.core.library.{LibraryServices, SubjectFactoryComponent}
import org.codefeedr.core.library.metastore.SubjectLibraryComponent
import org.codefeedr.model.{SubjectType, TrailedRecord}
import org.apache.flink.streaming.api.scala._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.async.Async.{async, await}

/**
  * Created by Niels on 31/07/2017.
  */
trait StreamComposerFactoryComponent {
  this: SubjectLibraryComponent with SubjectFactoryComponent =>

  val streamComposerFactory: StreamComposerFactory

  class StreamComposerFactory extends LazyLogging {

    def getComposer(query: QueryTree): Future[StreamComposer] = {

      query match {
        case SubjectSource(subjectName) =>
          logger.debug(s"Creating composer for subjectsource $subjectName")
          async {
            logger.debug(s"Waiting registration of subject $subjectName")
            val childNode = await(subjectLibrary.getSubjects().awaitChildNode(subjectName))
            logger.debug(s"Got subject $subjectName. Retrieving data")
            val subject = await(childNode.getData()).get
            new SourceStreamComposer(subject)
          }
        case Join(left, right, keysLeft, keysRight, selectLeft, selectRight, alias) =>
          logger.debug(s"Creating composer for join $alias")
          for {
            leftComposer <- getComposer(left)
            rightComposer <- getComposer(right)
            joinedType <- subjectLibrary
              .getSubject(alias)
              .getOrCreate(
                () =>
                  JoinQueryComposer.buildComposedType(leftComposer.getExposedType(),
                                                      rightComposer.getExposedType(),
                                                      selectLeft,
                                                      selectRight,
                                                      alias))
          } yield
            new JoinQueryComposer(leftComposer,
                                  rightComposer,
                                  joinedType,
                                  query.asInstanceOf[Join])
        case _ => {
          val error = new NotImplementedError("not implemented query subtree")
          throw error
        }
      }
    }

    /**
      * Created by Niels on 31/07/2017.
      */
    class SourceStreamComposer(subjectType: SubjectType) extends StreamComposer {

      //HACK: hard coded id
      override def compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord] = {
        env.addSource(
          subjectFactory.getSource(subjectLibrary.getSubject(subjectType.name),
                                   s"composedsink_${subjectType.name}"))
      }

      /**
        * Retrieve typeinformation of the type that is exposed by the Streamcomposer (Note that these types are not necessarily registered on kafka, as it might be an intermediate type)
        *
        * @return Typeinformation of the type exposed by the stream
        */
      override def getExposedType(): SubjectType = subjectType
    }
  }

}
