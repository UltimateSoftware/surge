// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

import sbt.Keys._

scalaVersion in ThisBuild := "2.12.8"

ThisBuild / crossScalaVersions := Seq("2.13.4", "2.12.8")

skip in publish := true

lazy val unitTest = taskKey[Unit]("Runs only the unit tests")

val multiJvmTestSettings = Seq(
  unitTest := {
    implicit val display: Show[Def.ScopedKey[_]] = Project.showContextKey(state.value)
    val testResultLogger = TestResultLogger.Default.copy(printNoTests = TestResultLogger.const(_ info "No tests to run for test:unitTest scope"))
    testResultLogger.run(streams.value.log, executeTests.in(Test).value, "test:unitTest")
  },
  // Override default definition of test so that sbt test runs both unit and multi-jvm tests
  test in Test := {
    unitTest.in(Test).value
    test.in(MultiJvm).value
  },
  dependencyOverrides ++= Dependencies.dependenciesOverride
)

lazy val `surge-scala-core` = (project in file("modules/scala-core"))
  .settings(
    libraryDependencies ++= Seq(
      jacksonKotlin,
      jacksonScala,
      java8Compat,
      Kafka.kafkaClients,
      PlayFramework.json,
      scalatest,
      typesafeConfig,
      mockitoCore
    )
  ).dependsOn(`surge-metrics`)

lazy val `surge-common` = (project in file("modules/common"))
  .settings(
    multiJvmTestSettings,
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.multiNodeTestkit,
      Akka.remote,
      Akka.jacksonSerialization,
      Alpakka.kafka,
      Alpakka.kafkaTestKit,
      Kafka.kafkaClients,
      Kafka.kafkaStreams,
      Kafka.kafkaStreamsScala,
      Kafka.kafkaStreamsTestUtils,
      PlayFramework.json,
      typesafeConfig,
      embeddedKafka,
      junit,
      logback,
      scalatest,
      scalatestPlusMockito,
      mockitoCore,
      jacksonKotlin
    )
  )
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .dependsOn(`surge-scala-core`)

lazy val `surge-rabbitmq-support` = (project in file ("modules/rabbit-support"))
  .settings(
    libraryDependencies ++= Seq(
      Alpakka.amqp,
      Akka.testKit,
      mockitoCore,
      scalatest,
      RabbitMq.embedded,
    )
  )
  .dependsOn(`surge-common`)

lazy val `surge-engine-command-core` = (project in file("modules/command-engine/core"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.remote,
      Akka.testKit,
      Kafka.kafkaClients,
      mockitoCore,
      scalatest,
      scalatestPlusMockito,
      logback,
      typesafeConfig
    )
  ).dependsOn(`surge-common`)

lazy val `surge-engine-command-scaladsl` = (project in file("modules/command-engine/scaladsl"))
  .dependsOn(`surge-engine-command-core`)

lazy val `surge-engine-command-javadsl` = (project in file("modules/command-engine/javadsl"))
  .dependsOn(`surge-engine-command-core`)

lazy val `surge-metrics` = (project in file("modules/metrics"))
  .settings(
    libraryDependencies ++= Seq(
      Kafka.kafkaClients,
      PlayFramework.json,
      slf4jApi,
      typesafeConfig,
      scalatest,
      scalatestPlusMockito,
      mockitoCore
    )
  )

lazy val `surge` = project.in(file("."))
  .aggregate(
    `surge-common`,
    `surge-engine-command-core`,
    `surge-engine-command-javadsl`,
    `surge-engine-command-scaladsl`,
    `surge-metrics`,
    `surge-scala-core`,
    `surge-rabbitmq-support`
  )
  .settings(
    skip in publish := true,
    aggregate in sonarScan := false,
    sonarUseExternalConfig := true
  )
