// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import sbt._

object Dependencies extends AutoPlugin {
  object autoImport {

    object Akka {
      val version = "2.5.25"

      val actor = "com.typesafe.akka" %% "akka-actor" % version
      val remote = "com.typesafe.akka" %% "akka-remote" % version
      val testKit = "com.typesafe.akka" %% "akka-testkit" % version % Test
    }

    object Kafka {
      val kafkaVersion = "2.3.1"

      val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion
    }

    object Ultimate {
      object Surge {
        val common = "com.ultimatesoftware" %% "surge-common" % "0.1.19"
        val scalaCore = "com.ultimatesoftware" %% "ulti-scala-core" % "0.1.8"
      }
    }

    val awaitility = "org.awaitility" % "awaitility" % "2.0.0"
    val mockitoCore = "org.mockito" % "mockito-core" % "2.25.1" % Test
    val scalatest = "org.scalatest" %% "scalatest" % "3.0.7" % Test
    val typesafeConfig = "com.typesafe" % "config" % "1.3.3"
  }
}
