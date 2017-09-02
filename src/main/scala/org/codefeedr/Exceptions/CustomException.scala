

package org.codefeedr.Exceptions

case class ActiveSinkException(message: String) extends Exception(message)
case class ActiveSourceException(message: String) extends Exception(message)

case class SinkAlreadySubscribedException(message: String) extends Exception(message)
case class SourceAlreadySubscribedException(message: String) extends Exception(message)

case class SinkNotSubscribedException(message: String) extends Exception(message)
case class SourceNotSubscribedException(message: String) extends Exception(message)

case class TypeNameNotFoundException(message: String) extends Exception(message)
