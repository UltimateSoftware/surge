// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

import sbt._

object Dependencies extends AutoPlugin {
  object autoImport {

    object Akka {
      val version = "2.6.20"
      val akkaHttpVersion = "10.2.9"
      val alpakkaVersion = "2.1.1"
      val managementVersion = "1.1.2"

      val kafkaStream = "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaVersion
      val kafkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % alpakkaVersion % Test
      val kafkaClusterSharding = ("com.typesafe.akka" %% "akka-stream-kafka-cluster-sharding" % alpakkaVersion).excludeAll {
        ExclusionRule("com.typesafe.akka", "akka-cluster-typed")
        ExclusionRule("com.typesafe.akka", "akka-cluster-sharding-typed")
      }
      val clusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version
      val clusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % version
      val clusterTyped = "com.typesafe.akka" %% "akka-cluster-typed" % version

      val management = "com.lightbend.akka.management" %% "akka-management" % managementVersion
      val managementClusterHttp = "com.lightbend.akka.management" %% "akka-management-cluster-http" % managementVersion
      val managementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % managementVersion
      val discoveryKubernetesApi = "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % managementVersion
      val actor = "com.typesafe.akka" %% "akka-actor" % version
      val stream = "com.typesafe.akka" %% "akka-stream" % version
      val slf4j = "com.typesafe.akka" %% "akka-slf4j" % version
      val multiNodeTestkit = "com.typesafe.akka" %% "akka-multi-node-testkit" % version % Test
      val remote = "com.typesafe.akka" %% "akka-remote" % version
      val discovery = "com.typesafe.akka" %% "akka-discovery" % version
      val protobufV3 = "com.typesafe.akka" %% "akka-protobuf-v3" % version
      val http = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
      val testKit = "com.typesafe.akka" %% "akka-testkit" % version % Test
      val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % version % Test
      val jacksonSerialization = "com.typesafe.akka" %% "akka-serialization-jackson" % version
    }

    object Kafka {
      val kafkaVersion = "3.2.3"

      val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaVersion
      val kafkaStreams = "org.apache.kafka" % "kafka-streams" % kafkaVersion
      val kafkaStreamsScala = "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion
      val kafkaStreamsTestUtils = "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test

      val rocksDb = "org.rocksdb" % "rocksdbjni" % "6.29.5"
    }

    object OpenTelemetry {

      val version = "1.16.0"
      val api = "io.opentelemetry" % "opentelemetry-api" % version
      val sdk = "io.opentelemetry" % "opentelemetry-sdk" % version % Test
      val sdkTesting = "io.opentelemetry" % "opentelemetry-sdk-testing" % version % Test
      val grpcChannel = "io.grpc" % "grpc-netty-shaded" % "1.48.1" % Test

      object HoneycombSample {
        val sdk = OpenTelemetry.sdk
        val exporter = "io.opentelemetry" % "opentelemetry-exporter-otlp" % OpenTelemetry.version % Test
        val grpc = OpenTelemetry.grpcChannel
      }
      object JaegerSample {
        val sdk = OpenTelemetry.sdk
        val exporter = "io.opentelemetry" % "opentelemetry-exporter-jaeger" % OpenTelemetry.version % Test
        val grpc = OpenTelemetry.grpcChannel
      }
    }

    object PlayFramework {
      val json = "com.typesafe.play" %% "play-json" % "2.9.2"
    }

    val akkaHttpPlayJson = "de.heikoseeberger" %% "akka-http-play-json" % "1.38.2"
    val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
    val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
    val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % "3.3.1" % Test
    val junit = "junit" % "junit" % "4.13.2" % Test
    val logbackForTesting = "ch.qos.logback" % "logback-classic" % "1.4.4" % Test
    val json4s = "org.json4s" %% "json4s-native" % "4.0.5"
    val mockitoCore = "org.mockito" % "mockito-core" % "4.7.0"
    val scalatest = "org.scalatest" %% "scalatest" % "3.2.13" % Test
    val scalatestPlusMockito = "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test
    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.36"
    val typesafeConfig = "com.typesafe" % "config" % "1.4.2"
    val jsonpath = "com.jayway.jsonpath" % "json-path" % "2.7.0" % Test
  }
}
