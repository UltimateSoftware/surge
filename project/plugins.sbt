// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

// Scala code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

// Git Commands - Access to Git repo information
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")

// Static code analysis
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.0.1")

// Test Coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.2")

// Multi-JVM testing
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

// Dependency helper plugins
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.0.15")

// Docs Site plugins
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.9.1")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")

addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")
