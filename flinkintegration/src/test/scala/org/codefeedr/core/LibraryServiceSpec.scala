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

package org.codefeedr.core

import org.apache.flink.api.java.utils.ParameterTool
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll}

/**
  * Base spec class that has asyncflatspec with libraryServices
  */
class LibraryServiceSpec extends AsyncFlatSpec with BeforeAndAfterAll{

  val libraryServices = new IntegrationTestLibraryServices{}
  implicit lazy val zkClient = libraryServices.zkClient

  override def beforeAll(): Unit = {
    //Initialize the configuration component with the codefeedr.properties file
    libraryServices.configurationProvider.initConfiguration(ParameterTool.fromArgs(new Array[String](0)),null)
    super.beforeAll()
  }
}
