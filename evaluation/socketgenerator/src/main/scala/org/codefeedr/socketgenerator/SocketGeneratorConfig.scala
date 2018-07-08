package org.codefeedr.socketgenerator

case class SocketGeneratorConfig(
                                hostname: String,
                                port: Int,
                                backlog: Int,
                                rate: Int,
                                totalEvents: Int
                                )
