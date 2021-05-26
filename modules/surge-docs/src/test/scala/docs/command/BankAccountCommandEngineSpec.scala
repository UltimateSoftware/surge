// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import java.util.UUID

import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.scaladsl.command.{ CommandFailure, CommandSuccess }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

class BankAccountCommandEngineSpec extends AnyWordSpec with Matchers with EmbeddedKafka {
  implicit val ec: ExecutionContext = ExecutionContext.global
  private val config = ConfigFactory.load()
  private val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = config.getInt("kafka.port"), zooKeeperPort = 0)

  "BankAccountCommandEngine" should {
    "Properly handle commands" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        createCustomTopic(BankAccountSurgeModel.eventsTopic.name, partitions = 5)
        createCustomTopic(
          BankAccountSurgeModel.stateTopic.name,
          partitions = 5,
          topicConfig = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))

        // #sending_command_to_engine
        val accountNumber = UUID.randomUUID()
        val createAccount = CreateAccount(accountNumber, "Jane Doe", "1234", 1000.0)

        val createdAccount: Future[Option[BankAccount]] = BankAccountEngine.surgeEngine.aggregateFor(accountNumber).sendCommand(createAccount).map {
          case CommandSuccess(aggregateState) => aggregateState
          case CommandFailure(reason)         => throw reason
        }
        // #sending_command_to_engine

        Await.result(createdAccount, 15.seconds) shouldEqual Some(BankAccount(accountNumber, "Jane Doe", "1234", 1000.0))

        // #sending_command_to_engine
        val creditAccount = CreditAccount(accountNumber, 100.0)

        val creditedAccount: Future[Option[BankAccount]] = BankAccountEngine.surgeEngine.aggregateFor(accountNumber).sendCommand(creditAccount).map {
          case CommandSuccess(aggregateState) => aggregateState
          case CommandFailure(reason)         => throw reason
        }
        // #sending_command_to_engine
        Await.result(creditedAccount, 10.seconds).map(_.balance) shouldEqual Some(1100.0)

        // #getting_state_from_engine
        val currentState: Future[Option[BankAccount]] = BankAccountEngine.surgeEngine.aggregateFor(accountNumber).getState
        // #getting_state_from_engine

        Await.result(currentState, 10.seconds) shouldEqual Some(BankAccount(accountNumber, "Jane Doe", "1234", 1100.0))

        // #rebalance_listener
        val rebalanceListener = new BankAccountRebalanceListener
        BankAccountEngine.surgeEngine.registerRebalanceListener(rebalanceListener)
      // #rebalance_listener

      }
    }
  }
}
