package org.codefeedr.core.library

import com.typesafe.config.Config

trait ConfigFactoryComponent {
  val conf: Config
}
