package org.codefeedr.configuration

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.codefeedr.core.library.CodefeedrComponents
import org.scalatest.FlatSpec

import scala.util.{Failure, Success}
import scala.util.control.NonFatal


class TestConfigurationComponent extends FlinkConfigurationProviderComponent with Serializable {
  override val configurationProvider: ConfigurationProvider = new ConfigurationProviderImpl()
}





class CodefeedrConfigurationImplSpec extends FlatSpec {
  "CodefeedrConfiguration" should "be serializable" in {
    //Arrange
    val config = new TestConfigurationComponent()

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

  "CodefeedrComponents" should "be serializable" in {
      //Arrange
      val c = new CodefeedrComponents {}

    //Act
    //Not catching the exception is on purpose
    //because you want the full stack trace of non serializable in case of a failure
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(c)
    oos.close()
    val t =Success(stream.toByteArray)

    //Assert
    assert(t.isSuccess)
  }


}



