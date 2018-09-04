package org.codefeedr.socketreceiver

import org.codefeedr.core.library.CodefeedrComponents

trait SocketReceiverModule extends
  CodefeedrComponents
  with SocketReceiverComponent {
  @transient lazy override val socketReceiver: SocketReceiver = new SocketReceiver()
}
