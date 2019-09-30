/*
 * Copyright (C) 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>
 */

import sbt._

object Dependencies extends AutoPlugin {
  object autoImport {

    object Kafka {
      val kafkaVersion = "2.1.1"

      val kafkaStreams = "org.apache.kafka" % "kafka-streams" % kafkaVersion
      val kafkaStreamsTestUtils = "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion
    }

    object Ultimate {
      object Surge {
        val akka = "com.ultimatesoftware" %% "surge-akka" % "0.0.4"
        val kafkaStreams = "com.ultimatesoftware" %% "surge-kafka-streams" % "0.0.4"
        val kafkaStreamsPlusAkka = "com.ultimatesoftware" %% "surge-ks-plus-akka" % "0.0.4"
        val scalaCore = "com.ultimatesoftware" %% "ulti-scala-core" % "0.0.4"
      }
    }

    val mockitoCore = "org.mockito" % "mockito-core" % "2.25.1" % Test
    val scalatest = "org.scalatest" %% "scalatest" % "3.0.7" % Test
    val typesafeConfig = "com.typesafe" % "config" % "1.3.3"
  }
}
