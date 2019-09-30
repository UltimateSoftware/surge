/*
 * Copyright (C) 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>
 */

import Keys._

lazy val `surge-engine-ks-command-core` = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
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
