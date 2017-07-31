package org.codefeedr.Library.Internal

import org.codefeedr.Library.SubjectLibrary
import org.codefeedr.Model.{Record, SubjectType}

import scala.reflect.ClassTag

/**
  * Created by Niels on 28/07/2017.
  * Utility class for some subjectType
  */
class RecordUtils(subjectType: SubjectType) {
  /**
    * Get a property of the given name and type on a record
    * Not optimized, but easy to use
    * @param property The name of the property
    * @param record The record to retrieve the property from
    * @tparam TValue Expected type of the property
    * @return The value
    * @throws Exception when the property was not found, of a different type or the record type has not yet been registered in the library
    */
  def getValueT[TValue: ClassTag](property: String)(implicit record: Record): TValue =
    getValue(property)(record).asInstanceOf[TValue]

  /**
    * Get a property of the given name and type on a record
    * Not optimized, but easy to use
    * @param property The name of the property
    * @param record The record to retrieve the property from
    * @return The value
    * @throws Exception when the property was not found or the record type has not yet been registered in the library
    */
  def getValue(property: String)(implicit record: Record): Any = {
    val subjectUuid = record.data(0).asInstanceOf[String]
    val propertyIndex = subjectType.properties
      .indexWhere(o => o.name == property)
    if (propertyIndex == -1) {
      throw new Exception(s"Property $propertyIndex was not found on type ${subjectType.name}")
    }
    record.data(propertyIndex)
  }
}
