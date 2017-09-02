

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

package org.codefeedr.Core.Library.Internal.Zookeeper

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

/**
  * UtilityClass
  * @param path
  */
class ZkNode(path: String) {
  /**
    * Name of the node
    */
  val Name: String = path.split('/').last

  def GetData[T: ClassTag]():Future[T] = ZkClient.GetData[T](path)
  def GetChild(name: String):ZkNode = new ZkNode(s"$path/$name")
  def GetChildren():Future[List[ZkNode]] = ZkClient.GetChildren(path).map(o => o.map(p => new ZkNode(p)))

  /**
    * Creates a future that watches the node until the data matches the given condition
    * @param condition Condition that should be true for the future to resolve
    * @tparam T Type of the node
    * @return a future that resolves when the given condition is true
    */
  def AwaitCondition[T](condition: T => Boolean): Future[T] = ZkClient.AwaitCondition(path, condition)
  def AwaitChild[T](child: String): Future[T] = ZkClient.AwaitChild(path, child)

}

object ZkNode {
  def apply(path: String) = new ZkNode(path)
}
