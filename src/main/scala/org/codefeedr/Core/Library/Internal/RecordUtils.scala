

package org.codefeedr.Core.Library.Internal

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
    val propertyIndex = subjectType.properties
      .indexWhere(o => o.name == property)
    if (propertyIndex == -1) {
      throw new Exception(s"Property $propertyIndex was not found on type ${subjectType.name}")
    }
    record.data(propertyIndex)
  }

  /**
    * Retrieve the respective indices of the properties on
    * @param properties Properties to find on the subject
    * @return array of indices, sorted in the same order as the given properties array
    */
  def getIndices(properties: Array[String]): Array[Int] = {
    val r = properties.map(prop => subjectType.properties.indexWhere(o => o.name == prop))
    if (r.contains(-1)) {
      throw new Exception(s"Some properties given to getSubjectType did not exist: ${properties
        .filter(o => !subjectType.properties.exists(p => p.name == o))
        .mkString(", ")}")
    }
    r
  }
}
