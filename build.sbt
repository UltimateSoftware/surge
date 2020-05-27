// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import Keys._

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
      Ultimate.Surge.scalaCore,
      typesafeConfig,
      embeddedKafka,
      junit,
      logback,
      scalatest,
      scalatestPlusMockito,
      mockitoCore,
      jacksonKotlin
    )
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
      scalatestPlusMockito,
      logback,
      typesafeConfig,
      Ultimate.Surge.scalaCore
    )
  ).dependsOn(`surge-common`)

lazy val `surge-engine-ks-command-scaladsl` = (project in file("modules/command-engine/scaladsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-engine-ks-command-javadsl` = (project in file("modules/command-engine/javadsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-engine-ks-query-core` = (project in file("modules/query-engine/core"))
  .settings(
    multiJvmTestSettings,
    jvmOptions in MultiJvm := Seq("-Dmultinode.server-port=4712"),
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.testKit,
      scalatest,
      scalatestPlusMockito,
      logback,
      mockitoCore
    )
  ).dependsOn(`surge-common` % "compile->compile;test->test")
   .enablePlugins(MultiJvmPlugin)
   .configs(MultiJvm)

lazy val `surge-kafka-streams` = project.in(file("."))
  .aggregate(
    `surge-common`,
    `surge-engine-ks-command-core`,
    `surge-engine-ks-command-javadsl`,
    `surge-engine-ks-command-scaladsl`,
    `surge-engine-ks-query-core`
  )
  .settings(
    skip in publish := true,
    aggregate in sonarScan := false,
    sonarUseExternalConfig := true
  )
