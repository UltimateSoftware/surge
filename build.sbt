// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import Keys.{_}

skip in publish := true

lazy val `surge-engine-ks-command-core` = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
      Akka.actor,
      Akka.remote,
      Akka.testKit,
      Kafka.kafkaClients,
      mockitoCore,
      scalatest,
      typesafeConfig,
      Ultimate.Surge.common,
      Ultimate.Surge.scalaCore,
      "com.ultimatesoftware.mp" % "messaging-platform-serialization" % "1.0.1" // TODO break this dependency by adding a module specific for the ulti-layering
    )
  )

lazy val `surge-engine-ks-command-scaladsl` = (project in file("scaladsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-engine-ks-command-javadsl` = (project in file("javadsl"))
  .dependsOn(`surge-engine-ks-command-core`)

lazy val `surge-test-engine-ks-command-javadsl` = (project in file("test-engine-javadsl"))
  .dependsOn(`surge-engine-ks-command-javadsl`)
  .settings(
    libraryDependencies ++= Seq(
      awaitility
    )
  )

lazy val root = project
  .aggregate(
    `surge-engine-ks-command-core`,
    `surge-engine-ks-command-javadsl`,
    `surge-engine-ks-command-scaladsl`,
    `surge-test-engine-ks-command-javadsl`
  )
  .settings(
    skip in publish := true
  )
