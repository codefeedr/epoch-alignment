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

package org.codefeedr.Core.Library.Internal.Kafka

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.codefeedr.Core.Engine.Query.SubjectSource
import org.codefeedr.Core.Library.SubjectFactory
import org.codefeedr.Model.{SubjectType, TrailedRecord}
import org.apache.flink.api.common.functions.MapFunction

import org.apache.flink.streaming.api.scala._

class KafkaTableSource(subjectType: SubjectType) extends StreamTableSource[Row] {
  val source: SourceFunction[TrailedRecord] = SubjectFactory.GetSource(subjectType)

  //Map the TrailedRecord provided by the source to a row
  override def getDataStream(execEnv: StreamExecutionEnvironment) = {
    //<3 the java API (not)
    execEnv
      .addSource(source)
      .map(
        new MapFunction[TrailedRecord, Row]() {
          override def map(value: TrailedRecord): Row = value.row
        }
      )
  }

  override def getReturnType = ???
}
