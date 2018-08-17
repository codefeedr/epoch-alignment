package org.codefeedr.util

import com.typesafe.config.Config

import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar


/**
  * Simple mockbuilder for the configuration component
  * Expand implementation as needed
  */
class ConfigFactoryComponentMock extends ConfigFactoryComponent with  MockitoSugar with MockitoExtensions {

  def addConfig(path: String, value: Int): Unit = {
    when(conf.getInt(path)) thenReturn value
  }

  override val conf: Config = mock[Config]
}
