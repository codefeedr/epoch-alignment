

package org.codefeedr.Core.Engine.Query

/**
  * Classes that represent a query execution tree
  * Created by Niels on 31/07/2017.
  */
abstract class QueryTree

case class SubjectSource(subjectType: String) extends QueryTree

case class Join(left: QueryTree,
                right: QueryTree,
                columnsLeft: Array[String],
                columnsRight: Array[String],
                SelectLeft: Array[String],
                SelectRight: Array[String],
                alias: String)
    extends QueryTree
