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

package org.codefeedr.Core.Engine.Query

import com.typesafe.scalalogging.Logger
import org.codefeedr.Core.Library.LibraryServices

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.async.Async.{async, await}

/**
  * Created by Niels on 31/07/2017.
  */
trait StreamComposerFactoryFacade {
  this: LibraryServices =>

  @transient private lazy val logger = Logger(classOf[StreamComposerFactoryFacade])


  def GetComposer(query: QueryTree): Future[StreamComposer] = {



    query match {
      case SubjectSource(subjectName) =>
        logger.debug(s"Creating composer for subjectsource $subjectName")
        async {
          logger.debug(s"Waiting registration of subject $subjectName")
          val childNode = await(subjectLibrary.GetSubjects().AwaitChildNode(subjectName))
          logger.debug(s"Got subject $subjectName. Retrieving data")
          val subject = await(childNode.GetData()).get
          new SourceStreamComposer(subject)
        }
      case Join(left, right, keysLeft, keysRight, selectLeft, selectRight, alias) =>
        logger.debug(s"Creating composer for join $alias")
        for {
          leftComposer <- GetComposer(left)
          rightComposer <- GetComposer(right)
          joinedType <- subjectLibrary
            .GetSubject(alias)
            .GetOrCreate(
              () =>
                JoinQueryComposer.buildComposedType(leftComposer.GetExposedType(),
                                                    rightComposer.GetExposedType(),
                                                    selectLeft,
                                                    selectRight,
                                                    alias))
        } yield
          new JoinQueryComposer(leftComposer, rightComposer, joinedType, query.asInstanceOf[Join])
      case _ => {
        val error = new NotImplementedError("not implemented query subtree")
        throw error
      }
    }
  }
}

object StreamComposerFactory extends StreamComposerFactoryFacade with LibraryServices
