// Copyright (C) 2018 Ultimate Software

import Keys._

skip in publish := true

lazy val `surge-engine-ks-command-core` = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.remote,
      Akka.testKit,
      Kafka.kafkaStreams,
      Kafka.kafkaStreamsTestUtils,
      mockitoCore,
      scalatest,
      typesafeConfig,
      Ultimate.Surge.akka,
      Ultimate.Surge.kafkaStreams,
      Ultimate.Surge.kafkaStreamsPlusAkka,
      Ultimate.Surge.scalaCore
    )
  )

lazy val `surge-engine-ks-command-scaladsl` = (project in file("scaladsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-engine-ks-command-javadsl` = (project in file("javadsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val root = project
  .aggregate(
    `surge-engine-ks-command-core`,
    `surge-engine-ks-command-javadsl`,
    `surge-engine-ks-command-scaladsl`
  )
  .settings(
    skip in publish := true
  )
