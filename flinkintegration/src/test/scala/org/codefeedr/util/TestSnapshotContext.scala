package org.codefeedr.util

import java.{io, lang, util}

import org.apache.flink.api.common.state._
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import scala.collection.JavaConverters._


/**
  * Classes used to provide mocked states to functions
  * TODO: Implementation is incomplete, expand functionality as needed
  * @param isRestored boolean indicating if the function is restored
  */
class TestFunctionInitializationContext(val isRestored:Boolean)(val operatorStateStore: TestOperatorStateStore) extends FunctionInitializationContext{
  override def getOperatorStateStore: OperatorStateStore = operatorStateStore

  override def getKeyedStateStore: KeyedStateStore = ???
}

object TestFunctionInitializationContext{
  def apply[S](isRestored:Boolean,listState:TestListState[S]) = {
    new TestFunctionInitializationContext(isRestored)(new TestOperatorStateStore(listState))
  }

}

/**
  * Implementation of the operator state store to unit test sources
  * TODO: Implementation is incomplete, expand as needed
  * @param state the list state
  */
class TestOperatorStateStore(val state:State) extends OperatorStateStore {

  override def getBroadcastState[K, V](stateDescriptor: MapStateDescriptor[K, V]): BroadcastState[K, V] = ???

  override def getListState[S](stateDescriptor: ListStateDescriptor[S]): TestListState[S] = state.asInstanceOf[TestListState[S]]

  override def getUnionListState[S](stateDescriptor: ListStateDescriptor[S]): TestListState[S] = ???

  override def getRegisteredStateNames: util.Set[String] = ???

  override def getRegisteredBroadcastStateNames: util.Set[String] = ???

  override def getOperatorState[S](stateDescriptor: ListStateDescriptor[S]): TestListState[S] = ???

  override def getSerializableListState[T <: io.Serializable](stateName: String): TestListState[T] = ???
}


/**
  * Test list state
  * @tparam S
  */
class TestListState[S] extends ListState[S] {
  @volatile private var state:List[S] = List.empty[S]

  override def update(values: util.List[S]): Unit = state = values.asScala.toList

  override def addAll(values: util.List[S]): Unit = state =state ++ values.asScala

  override def get(): lang.Iterable[S] =state.asJava

  override def add(value: S): Unit = {
    state = value :: state
  }

  override def clear(): Unit = state = List.empty[S]
}

class TestSnapshotContext(val checkpointId:Long,val checkpointTimestamp:Long = 0) extends FunctionSnapshotContext {
  override def getCheckpointId: Long = checkpointId

  override def getCheckpointTimestamp: Long = checkpointTimestamp
}