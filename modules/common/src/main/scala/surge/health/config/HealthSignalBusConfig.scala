// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.config

case class HealthSignalBusConfig(streamingEnabled: Boolean, signalTopic: String, registrationTopic: String, allowedSubscriberCount: Int)
