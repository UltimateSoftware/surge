// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.cluster

import akka.actor.{ Address, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }

/**
 * This extension wraps an actor system and exposes the underlying address for that actor system,
 * which allows us to ask the actor system directly on what host/port it is running rather than
 * trying to pull this from configuration settings in different places.
 *
 * @param system The underlying actor system
 */
class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
  def address: Address = system.provider.getDefaultAddress
}
object RemoteAddressExtension extends ExtensionId[RemoteAddressExtensionImpl]
  with ExtensionIdProvider {
  override def lookup: ExtensionId[RemoteAddressExtensionImpl] = RemoteAddressExtension
  override def createExtension(system: ExtendedActorSystem): RemoteAddressExtensionImpl = new RemoteAddressExtensionImpl(system)
}
