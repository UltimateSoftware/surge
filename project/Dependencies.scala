// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

import sbt.Keys.libraryDependencies
import sbt._

object Dependencies extends AutoPlugin {
  object autoImport {

    object Akka {
      val version = "2.6.9"

      val actor = "com.typesafe.akka" %% "akka-actor" % version
      val multiNodeTestkit = "com.typesafe.akka" %% "akka-multi-node-testkit" % version % Test
      val remote = "com.typesafe.akka" %% "akka-remote" % version
      val testKit = "com.typesafe.akka" %% "akka-testkit" % version % Test
      val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % version % Test
      val jacksonSerialization = "com.typesafe.akka" %% "akka-serialization-jackson" % version
    }

    object Alpakka {
      val alpakkaVersion = "2.0.2"
      val amqp = "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % alpakkaVersion
      val kafka = "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaVersion
      val kafkaTestKit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % alpakkaVersion % Test
    }

    object Kafka {
      val kafkaVersion = "2.7.0"

      val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion
      val kafkaStreams = "org.apache.kafka" % "kafka-streams" % kafkaVersion
      val kafkaStreamsScala = "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion
      val kafkaStreamsTestUtils = "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test
    }

    object OpenTelemetry {
      val version = "0.33.0"
      val api = "io.opentelemetry" % "opentelemetry-api" % "1.4.1"
    }

    object PlayFramework {
      val json = "com.typesafe.play" %% "play-json" % "2.9.1"
    }

    object RabbitMq {
      val embedded = "io.arivera.oss" % "embedded-rabbitmq" % "1.4.0" % Test
    }

    val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.2"
    val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
    val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % Kafka.kafkaVersion % Test
    val junit = "junit" % "junit" % "4.13.1" % Test
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
    val mockitoCore = "org.mockito" % "mockito-core" % "3.6.28" % Test
    val scalatest = "org.scalatest" %% "scalatest" % "3.2.7" % Test
    val scalatestPlusMockito = "org.scalatestplus" %% "mockito-3-4" % "3.2.7.0" % Test
    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.30"
    val typesafeConfig = "com.typesafe" % "config" % "1.4.1"
  }
}
