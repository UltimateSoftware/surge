// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.NotUsed
import akka.kafka.Subscriptions
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.stream.scaladsl.Flow
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentracing.Tracer
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, OptionValues }
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.kafka.KafkaTopic
import surge.kafka.streams.DefaultSerdes
import surge.streams.{ EventPlusStreamMeta, KafkaStreamMeta }
import surge.streams.replay.{ DefaultEventReplaySettings, KafkaForeverReplaySettings, KafkaForeverReplayStrategy, NoOpEventReplayStrategy }
import io.opentracing.noop.NoopTracerFactory

import scala.concurrent.duration._
