package org.codefeedr.configuration
import scala.concurrent.{Await, Future}

trait ConfigUtilComponent { this: ConfigurationProviderComponent =>

  val configUtil: ConfigUtil

  def awaitReady[T](f: Future[T]) = configUtil.awaitReady(f)

  class ConfigUtil {
    def awaitReady[T](f: Future[T]): T =
      Await.result(f, configurationProvider.getDefaultAwaitDuration)
  }

}
