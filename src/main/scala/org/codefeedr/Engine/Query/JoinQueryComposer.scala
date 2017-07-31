/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.codefeedr.Engine.Query
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.Library.Internal.RecordUtils
import org.codefeedr.Model.{SubjectType, TrailedRecord}

/**
  * Created by Niels on 31/07/2017.
  */
class JoinQueryComposer(leftComposer: StreamComposer, rightComposer: StreamComposer, keysLeft: Array[String], keysRight: Array[String], selectLeft: Array[String], selectRight: Array[String]) extends StreamComposer{
  override def Compose(env: StreamExecutionEnvironment): DataStream[TrailedRecord] = {
    val leftStream = leftComposer.Compose(env)
    val rightStream = rightComposer.Compose(env)

    val leftMapper = getJoinKeyMapper(keysLeft, leftComposer.GetExposedType())
    val mappedLeft = leftStream.map(o => Tuple2(leftMapper(o),o))
    val rightMapper = getJoinKeyMapper(keysRight, rightComposer.GetExposedType())
    val mappedRight = rightStream.map(o => Tuple2(rightMapper(o),o))
    val union = mappedLeft.union(mappedRight)


    





  }


  def getJoinKeyMapper(properties: Array[String], subjectType: SubjectType): (TrailedRecord) => Array[Any] = {
    val indices = new RecordUtils(subjectType).getIndices(properties)
    (r: TrailedRecord) => for(i <- indices) yield r.record.data(i)

  }





}
