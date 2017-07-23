package org.codefeedr.Library.Internal

import org.codefeedr.Model.{ActionType, Record}

import scala.reflect.runtime.{universe => ru}

/**
  * Created by Niels on 23/07/2017.
  * This class can transform objects of any type into records used in the query engine
  * This class is not serializable and should be constructed as needed
  */

class Bagger[TData: ru.TypeTag] {
  //Fetch type information
  private val subjectType = SubjectTypeFactory.getSubjectType[TData]

  /**
    * Build array of accessors for a field only once, using the type information distributed by kafka
    * Make sure to crash whenever there is a mismatch between distributed typeinformation and actual object type
    */
  private val accessors = subjectType.properties
    .map(o => ru.typeOf[TData].getClass.getDeclaredField(o.name))
    .map(o => {
      o.setAccessible(true)
      (obj: TData) => o.get(obj)
    })


  /**
    * Bag a generic object into a record used in the query evaluation
    * @param data The object to bag
    * @param action Type of the action (Add, Update, Delete)
    * @return The record that can be pushed into the query engine
    */
  def Bag(data: TData, action: ActionType.Value): Record = {
    Record(accessors.map(o=>o(data)),action)
  }
}
