// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

import Dependencies.autoImport.OpenTelemetry.{ HoneycombSample, JaegerSample }
import sbt.Keys._

ThisBuild / scalaVersion := "2.13.5"

ThisBuild / crossScalaVersions := Seq("2.13.5", "2.12.12")

publish / skip := true

lazy val unitTest = taskKey[Unit]("Runs only the unit tests")

val multiJvmTestSettings = Seq(
  unitTest := {
    implicit val display: Show[Def.ScopedKey[_]] = Project.showContextKey(state.value)
    val testResultLogger = TestResultLogger.Default.copy(printNoTests = TestResultLogger.const(_.info("No tests to run for test:unitTest scope")))
    testResultLogger.run(streams.value.log, (Test / executeTests).value, "test:unitTest")
  },
  // Override default definition of test so that sbt test runs both unit and multi-jvm tests
  Test / test := {
    (Test / unitTest).value
    // FIXME fix multi-jvm tests running on GH Actions
    // (MultiJvm / test).value
  })

lazy val `surge-serialization` =
  (project in file("modules/serialization"))
    .settings(multiJvmTestSettings, libraryDependencies ++= Seq(scalaCollectionCompat, scalatest, scalatestPlusMockito, mockitoCore, PlayFramework.json))

lazy val `surge-common` = (project in file("modules/common"))
  .settings(
    multiJvmTestSettings,
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.multiNodeTestkit,
      Akka.remote,
      Akka.jacksonSerialization,
      Akka.clusterSharding,
      Alpakka.kafka,
      Alpakka.kafkaTestKit,
      Kafka.kafkaClients,
      Kafka.kafkaStreams,
      Kafka.kafkaStreamsScala,
      Kafka.kafkaStreamsTestUtils,
      OpenTelemetry.api,
      OpenTelemetry.sdk,
      OpenTelemetry.sdkTesting,
      PlayFramework.json,
      typesafeConfig,
      Akka.akkaStreamTestKit,
      embeddedKafka,
      junit,
      logback,
      scalatest,
      scalatestPlusMockito,
      mockitoCore))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .dependsOn(`surge-metrics`, `surge-serialization`)

lazy val `surge-engine-command-core` = (project in file("modules/command-engine/core"))
  .settings(libraryDependencies ++= Seq(
    Akka.actor,
    Akka.remote,
    Kafka.kafkaClients,
    Akka.testKit,
    Akka.akkaStreamTestKit,
    Akka.kafkaStream,
    Akka.kafkaClusterSharding,
    Akka.management,
    Akka.managementClusterHttp,
    Akka.http,
    mockitoCore,
    scalatest,
    scalatestPlusMockito,
    embeddedKafka,
    OpenTelemetry.api,
    logback,
    typesafeConfig))
  .dependsOn(`surge-common` % "compile->compile;test->test")

lazy val `surge-engine-command-scaladsl` = (project in file("modules/command-engine/scaladsl")).dependsOn(`surge-engine-command-core`)

lazy val `surge-engine-command-javadsl` =
  (project in file("modules/command-engine/javadsl"))
    .dependsOn(`surge-engine-command-core`)
    .settings(libraryDependencies ++= Seq(scalatest, scalatestPlusMockito, mockitoCore))

lazy val `surge-engine-multilanguage-protocol` =
  (project in file("modules/multilanguage-protocol")).enablePlugins(AkkaGrpcPlugin)

lazy val `surge-engine-multilanguage` =
  (project in file("modules/multilanguage"))
    .dependsOn(`surge-engine-command-scaladsl`, `surge-engine-multilanguage-protocol`)
    .settings(
      libraryDependencies ++= Seq(Akka.discovery, Akka.slf4j, Akka.http, logback, slf4jApi, Akka.testKit, scalatest, embeddedKafka),
      publish / skip := true)
    .enablePlugins(JavaServerAppPackaging)

lazy val `surge-engine-multilanguage-scala-sdk` =
  (project in file("modules/multilanguage-scala-sdk"))
    .settings(publish / skip := true)
    .dependsOn(`surge-engine-multilanguage-protocol`)
    .enablePlugins(AkkaGrpcPlugin)

lazy val `surge-engine-multilanguage-scala-sdk-sample` =
  (project in file("modules/multilanguage-scala-sdk-sample"))
    .settings(
      libraryDependencies ++= Seq(Akka.http, Akka.discovery, Akka.stream, Akka.protobufV3, Akka.slf4j, logback, slf4jApi, json4s),
      publish / skip := true)
    .dependsOn(`surge-engine-multilanguage-scala-sdk`)
    .enablePlugins(JavaServerAppPackaging)

lazy val `surge-metrics` = (project in file("modules/metrics")).settings(
  libraryDependencies ++= Seq(
    Akka.actor,
    Akka.testKit,
    Kafka.kafkaClients,
    PlayFramework.json,
    scalaCollectionCompat,
    slf4jApi,
    typesafeConfig,
    scalatest,
    scalatestPlusMockito,
    mockitoCore))

lazy val `surge-docs` = (project in file("modules/surge-docs"))
  .dependsOn(`surge-common`, `surge-engine-command-core`, `surge-engine-command-javadsl`, `surge-engine-command-scaladsl`, `surge-metrics`)
  .enablePlugins(ParadoxPlugin, ParadoxSitePlugin, GhpagesPlugin)
  .settings(
    javacOptions ++= Seq("-source", "15", "--enable-preview"),
    compileOrder := CompileOrder.JavaThenScala,
    publish / skip := true,
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    libraryDependencies ++= Seq(
      typesafeConfig,
      embeddedKafka,
      logback,
      scalatest,
      scalatestPlusMockito,
      mockitoCore,
      HoneycombSample.sdk,
      HoneycombSample.exporter,
      HoneycombSample.grpc,
      JaegerSample.sdk,
      JaegerSample.exporter,
      JaegerSample.grpc))

lazy val `surge` = project
  .in(file("."))
  .aggregate(
    `surge-serialization`,
    `surge-common`,
    `surge-engine-command-core`,
    `surge-engine-command-javadsl`,
    `surge-engine-command-scaladsl`,
    `surge-metrics`,
    `surge-docs`,
    `surge-engine-multilanguage-protocol`,
    `surge-engine-multilanguage`,
    `surge-engine-multilanguage-scala-sdk`,
    `surge-engine-multilanguage-scala-sdk-sample`)
  .settings(skip in publish := true, ReleaseSettings.settings)
  .disablePlugins(MimaPlugin)

addCommandAlias("codeFormat", ";headerCreate;test:headerCreate;scalafmtAll;scalafmtSbt")
