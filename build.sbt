// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import Keys._

skip in publish := true

lazy val unitTest = taskKey[Unit]("Runs only the unit tests")

lazy val `surge-common` = (project in file("modules/common"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.multiNodeTestkit,
      Akka.remote,
      Alpakka.kafka,
      Kafka.kafkaClients,
      Kafka.kafkaStreams,
      Kafka.kafkaStreamsScala,
      Kafka.kafkaStreamsTestUtils,
      PlayFramework.json,
      Ultimate.Surge.scalaCore,
      typesafeConfig,
      junit,
      logback,
      scalatest,
      mockitoCore
    ),

    unitTest := {
      implicit val display: Show[Def.ScopedKey[_]] = Project.showContextKey(state.value)
      val testResultLogger = TestResultLogger.Default.copy(printNoTests = TestResultLogger.const(_ info "No tests to run for test:unitTest scope"))
      testResultLogger.run(streams.value.log, executeTests.in(Test).value, "test:unitTest")
    },
    // Override default definition of test so that sbt test runs both unit and multi-jvm tests
    test in Test := {
      unitTest.in(Test).value
      test.in(MultiJvm).value
    }

  ).enablePlugins(MultiJvmPlugin)
   .configs(MultiJvm)

lazy val `surge-engine-ks-command-core` = (project in file("modules/command-engine/core"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.remote,
      Akka.testKit,
      Kafka.kafkaClients,
      mockitoCore,
      scalatest,
      typesafeConfig,
      Ultimate.Surge.scalaCore,
      "com.ultimatesoftware.mp" % "messaging-platform-serialization" % "1.0.1" // TODO break this dependency by adding a module specific for the ulti-layering
    )
  ).dependsOn(`surge-common`)

lazy val `surge-engine-ks-command-scaladsl` = (project in file("modules/command-engine/scaladsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-engine-ks-command-javadsl` = (project in file("modules/command-engine/javadsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-test-engine-ks-command-javadsl` = (project in file("modules/command-engine/test-engine-javadsl"))
  .dependsOn(`surge-engine-ks-command-javadsl`)
  .settings(
    libraryDependencies ++= Seq(
      awaitility,
      Ultimate.Surge.mpDomainHelpers
    )
  )

lazy val `surge-engine-ks-query-core` = (project in file("modules/query-engine/core"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.testKit,
      Kafka.kafkaStreams,
      Kafka.kafkaStreamsTestUtils,
      scalatest,
      mockitoCore
    )
  ).dependsOn(`surge-common`)

lazy val `surge-engine-ks-query-scaladsl` = (project in file("modules/query-engine/scaladsl"))
  .dependsOn(`surge-engine-ks-query-core`)

lazy val `surge-engine-ks-query-javadsl` = (project in file("modules/query-engine/javadsl"))
  .dependsOn(`surge-engine-ks-query-core`)

lazy val `surge-kafka-streams` = project.in(file("."))
  .aggregate(
    `surge-common`,
    `surge-engine-ks-command-core`,
    `surge-engine-ks-command-javadsl`,
    `surge-engine-ks-command-scaladsl`,
    `surge-test-engine-ks-command-javadsl`,
    `surge-engine-ks-query-core`,
    `surge-engine-ks-query-scaladsl`,
    `surge-engine-ks-query-javadsl`
  )
  .settings(
    skip in publish := true,
    aggregate in sonarScan := false,
    sonarUseExternalConfig := true
  )
