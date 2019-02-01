package org.codefeedr.core.library

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.scalatest.FlatSpec

import scala.util.Success

class TestComponent extends ConfigurationComponents with Serializable {

}

class LibraryServicesSpec extends FlatSpec {
  "SerializableComponents" should "be serializable" in {
    //Arrange
    val config = new TestComponent()

    //Act
    //Not catching the exception is on purpose
    //because you want the full stack trace of non serializable in case of a failure
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(config)
    oos.close()
    val t =Success(stream.toByteArray)

    //Assert
    assert(t.isSuccess)
  }
}