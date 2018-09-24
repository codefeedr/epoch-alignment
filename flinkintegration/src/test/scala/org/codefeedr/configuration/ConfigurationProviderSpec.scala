package org.codefeedr.configuration

import org.apache.flink.api.java.utils.ParameterTool
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._

class ConfigurationProviderSpec extends FlatSpec with FlinkConfigurationProviderComponent {


  "initConfiguration" should "read default values from codefeedr.properties" in {
    //Arrange
    val component = new ConfigurationProviderImpl()

    //Act
    val initialPt = ParameterTool.fromMap(Map.empty[String,String].asJava)
    component.initConfiguration(initialPt)

    //Assert
    assert(component.get("test.value") == "default")
    assert(component.get("test.defaultvalue") == "default")
  }

  it should "override values with a custom passed properties file" in {
    //Arrange
    val component = new ConfigurationProviderImpl()

    //Act
    val initialPt = ParameterTool.fromMap(Map(
      "propertiesFile" -> "/test.properties"
    ).asJava)
    component.initConfiguration(initialPt)

    //Assert
    assert(component.get("test.value") == "testvalue")
    assert(component.get("test.defaultvalue") == "default")
  }

  it should "override custom passed properties file with passed arguments" in {
    //Arrange
    val component = new ConfigurationProviderImpl()

    //Act
    val initialPt = ParameterTool.fromMap(Map(
      "propertiesFile" -> "/test.properties",
      "test.value" -> "override"
    ).asJava)
    component.initConfiguration(initialPt)

    //Assert
    assert(component.get("test.value") == "override")
    assert(component.get("test.defaultvalue") == "default")
  }



  override val configurationProvider: ConfigurationProvider =  new ConfigurationProviderImpl()
}
