// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

name := "sample-surge"

version := "0.1"

scalaVersion := "2.13.6"

val SurgeVersion = "0.5.33-SNAPSHOT"
val AkkaHttpVersion = "10.2.6"
val AkkaHttpJsonVersion = "1.38.2"

libraryDependencies ++= Seq(
  "com.ukg" %% "surge-engine-command-scaladsl" % SurgeVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "de.heikoseeberger" %% "akka-http-play-json" % AkkaHttpJsonVersion,
)
