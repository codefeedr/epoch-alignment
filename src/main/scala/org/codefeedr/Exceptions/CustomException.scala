package org.codefeedr.Exceptions

case class ActiveSinkException(message:String) extends Exception(message)
case class ActiveSourceException(message:String) extends Exception(message)