// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import java.time.Duration
import java.util.concurrent.TimeUnit

import io.arivera.oss.embedded.rabbitmq.bin.RabbitMqCommand
import io.arivera.oss.embedded.rabbitmq.{ EmbeddedRabbitMq, EmbeddedRabbitMqConfig, PredefinedVersion }
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration._

object EmbeddedRabbit {
  val SERVICE_PORT: Int = 5672
  val log: Logger = LoggerFactory.getLogger(getClass)
}

trait EmbeddedRabbit {
  import EmbeddedRabbit._
  def port(): Int = SERVICE_PORT
  def vhost(): String = "vhost"
  def user(): String = "tester"
  def timeout(): FiniteDuration = 10.seconds

  def rabbitMqConfig: EmbeddedRabbitMqConfig = new EmbeddedRabbitMqConfig.Builder().port(port())
    .downloadConnectionTimeoutInMillis(Duration.ofSeconds(timeout().toSeconds).toMillis)
    .defaultRabbitMqCtlTimeoutInMillis(Duration.ofSeconds(timeout().toSeconds).toMillis)
    .version(PredefinedVersion.V3_7_18)
    .rabbitMqServerInitializationTimeoutInMillis(Duration.ofSeconds(timeout().toSeconds).toMillis).build()

  val rabbitMq: EmbeddedRabbitMq = new EmbeddedRabbitMq(rabbitMqConfig)

  def rabbitMqUri(): UriInfo = {
    UriInfo.fromConfig(rabbitMqConfig, user(), vhost())
  }

  def startRabbit(): Unit = {
    rabbitMq.start()
    setupRabbit()
  }

  def stopRabbit(): Unit = {
    teardownRabbit()

    rabbitMq.stop()
  }

  private def teardownRabbit(): Unit = {
    val deleteUserCommand: RabbitMqCommand = new RabbitMqCommand(rabbitMqConfig, "rabbitmqctl", "delete_user", user())
    val deleteVHostCommand: RabbitMqCommand = new RabbitMqCommand(rabbitMqConfig, "rabbitmqctl", "delete_vhost", vhost())

    val deleteResult = deleteUserCommand.call().getFuture.get(timeout().toSeconds, TimeUnit.SECONDS)

    if (deleteResult.getExitValue == 0) {
      val hostResult = deleteVHostCommand.call().getFuture.get(timeout().toSeconds, TimeUnit.SECONDS)
      if (hostResult.getExitValue == 0) {
        log.info("RabbitMq teardown complete")
      } else {
        throw new RuntimeException("RabbitMq teardown failed - Virtual Host not removed")
      }
    } else {
      throw new RuntimeException("RabbitMq teardown failed - User not removed")
    }
  }

  private def setupRabbit(): Unit = {
    // Add Vhost
    val addVHostCommand: RabbitMqCommand = new RabbitMqCommand(rabbitMqConfig, "rabbitmqctl", "add_vhost", vhost())
    val addUserCommand: RabbitMqCommand = new RabbitMqCommand(rabbitMqConfig, "rabbitmqctl", "add_user", user(), user())
    val setPermissions: RabbitMqCommand = new RabbitMqCommand(rabbitMqConfig, "rabbitmqctl", "set_permissions", "-p", vhost(), user(), ".*", ".*", ".*")
    val addResult = addVHostCommand.call().getFuture.get(10, TimeUnit.SECONDS)

    if (addResult.getExitValue == 0) {
      val userResult = addUserCommand.call().getFuture.get(10, TimeUnit.SECONDS)
      if (userResult.getExitValue == 0) {
        val permissionResult = setPermissions.call().getFuture.get(10, TimeUnit.SECONDS)
        if (permissionResult.getExitValue == 0) {
          log.info("RabbitMq set up complete")
        } else {
          throw new RuntimeException("RabbitMq set up failed - Permissions not set")
        }
      } else {
        throw new RuntimeException("RabbitMq set up failed - User not added")
      }
    } else {
      throw new RuntimeException("RabbitMq set up failed - Virtual Host not added")
    }
  }
}
