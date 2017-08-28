package org.codefeedr.Core.Library.Internal

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.codefeedr.Model.{PropertyType, RecordProperty, SubjectType}
import scala.reflect.runtime.{universe => ru}

/**
  * Thread safe
  * Created by Niels on 14/07/2017.
  */
object SubjectTypeFactory extends LazyLogging {
  private def newTypeIdentifier(): UUID = UUID.randomUUID()

  private def getSubjectTypeInternal(t: ru.Type, idFields: Array[String]): SubjectType = {
    val properties = t.members.filter(o => !o.isMethod)
    val name = getSubjectName(t)
    val r = SubjectType(newTypeIdentifier().toString,
                        name,persistent = false,
                        properties = properties.map(getRecordProperty(idFields)).toArray)
    if (r.properties.count(o => o.id) != idFields.length) {
      throw new Exception(s"Some idfields given to getSubjectType did not exist: ${idFields
        .filter(o => !r.properties.map(o => o.name).contains(o))
        .mkString(", ")}")
    }
    r
  }

  private def getRecordProperty(idFields: Array[String])(symbol: ru.Symbol): RecordProperty = {
    val name = symbol.name.toString.trim
    //logger.debug(f"property type of $name: ${symbol.info.toString}")
    val propertyType = symbol.typeSignature.typeSymbol.name.toString match {
      case "scala.Int" => PropertyType.Number
      case "Int" => PropertyType.Number
      case "String" => PropertyType.String
      case _ => PropertyType.Any
    }

    RecordProperty(name, propertyType, idFields.contains(name))
  }

  /**
    * Use a generic type to retrieve the subjectName that the given type would produce
    * @tparam T the type to retrieve th name for
    * @return the name
    */
  def getSubjectName[T: ru.TypeTag]: String = getSubjectName(ru.typeOf[T])

  /**
    * Use the type to retrieve the subjectName that the given type woul produce
    * @param t the type to retrieve name for
    * @return the name of the subject
    */
  def getSubjectName(t: ru.Type): String = t.typeSymbol.name.toString

  /**
    * Get a subject type for the query language, type tag required
    * @tparam T type of the subject
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag]: SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], Array.empty[String])

  /**
    * Get subject type for the query language, type tag required
    * @param idFields Names of the fields that uniquely identify the record
    * @tparam T Type of the object
    * @return Type description of the given type
    */
  def getSubjectType[T: ru.TypeTag](idFields: Array[String]): SubjectType =
    getSubjectTypeInternal(ru.typeOf[T], idFields)

}
