// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import sbt._

object Dependencies extends AutoPlugin {
  object autoImport {

    object Akka {
      val version = "2.5.25"

      val actor = "com.typesafe.akka" %% "akka-actor" % version
      val multiNodeTestkit = "com.typesafe.akka" %% "akka-multi-node-testkit" % version % Test
      val remote = "com.typesafe.akka" %% "akka-remote" % version
      val testKit = "com.typesafe.akka" %% "akka-testkit" % version % Test
    }

    object Alpakka {
      val alpakkaVersion = "2.0.2"
      val kafka = "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaVersion
      val kafkaTestKit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % alpakkaVersion % Test
    }

    object Kafka {
      val kafkaVersion = "2.3.1"

      val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion
      val kafkaStreams = "org.apache.kafka" % "kafka-streams" % kafkaVersion
      val kafkaStreamsScala = "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion
      val kafkaStreamsTestUtils = "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test
    }

    object PlayFramework {
      val jsonVersion = "2.7.2"
      val json = "com.typesafe.play" %% "play-json" % jsonVersion
    }

    object Ultimate {
      object Surge {
        val scalaCore = "com.ultimatesoftware" %% "ulti-scala-core" % "0.3.7"
      }
    }

    val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % "2.4.0" % Test
    val junit = "junit" % "junit" % "4.13" % Test
    val logback =  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
    val mockitoCore = "org.mockito" % "mockito-core" % "3.3.3" % Test
    val scalatest = "org.scalatest" %% "scalatest" % "3.0.8" % Test
    val typesafeConfig = "com.typesafe" % "config" % "1.3.3"
  }
}
