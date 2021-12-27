// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import java.util.UUID
import com.typesafe.config.ConfigFactory
import io.github.embeddedkafka.EmbeddedKafka.createCustomTopic
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec
import surge.scaladsl.common.{ CommandFailure, CommandSuccess }

import scala.concurrent.{ ExecutionContext, Future }

class BankAccountCommandEngineSpec extends AnyWordSpec with BeforeAndAfterAll with Matchers with ScalaFutures {
  implicit val ec: ExecutionContext = ExecutionContext.global
  private val config = ConfigFactory.load()
  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = config.getInt("kafka.port"), zooKeeperPort = 0)
  override def beforeAll(): Unit = {
    BankAccountEngine.surgeEngine.start()
  }

  override def afterAll(): Unit = {
    BankAccountEngine.surgeEngine.stop()
  }

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Milliseconds))

  "BankAccountCommandEngine" should {
    "Properly handle commands" in {
      EmbeddedKafka.start()
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

      createdAccount.futureValue shouldEqual Some(BankAccount(accountNumber, "Jane Doe", "1234", 1000.0))

      // #sending_command_to_engine
      val creditAccount = CreditAccount(accountNumber, 100.0)

      val creditedAccount: Future[Option[BankAccount]] = BankAccountEngine.surgeEngine.aggregateFor(accountNumber).sendCommand(creditAccount).map {
        case CommandSuccess(aggregateState) => aggregateState
        case CommandFailure(reason)         => throw reason
      }
      // #sending_command_to_engine
      creditedAccount.futureValue.map(_.balance) shouldEqual Some(1100.0)

      // #getting_state_from_engine
      val currentState: Future[Option[BankAccount]] = BankAccountEngine.surgeEngine.aggregateFor(accountNumber).getState
      // #getting_state_from_engine

      currentState.futureValue shouldEqual Some(BankAccount(accountNumber, "Jane Doe", "1234", 1100.0))

      // #rebalance_listener
      val rebalanceListener = new BankAccountRebalanceListener
      BankAccountEngine.surgeEngine.registerRebalanceListener(rebalanceListener)
      // #rebalance_listener
      EmbeddedKafka.stop()
    }
  }
}
