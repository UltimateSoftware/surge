// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

resolvers ++= Seq(
  "gears-tools-generic-release" at "https://artifactory.mia.ulti.io/artifactory/gt-generic-prod",
  "gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/"
)
// Scalariform - Scala code formatting
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")

// Git Commands - Access to Git repo information
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")

// Static code analysis
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Test Coverage
addSbtPlugin("com.ultimatesoftware.scoverage" % "sbt-scoverage" % "1.6.2")
addSbtPlugin("com.github.mwz" % "sbt-sonar" % "2.1.0")

// Multi-JVM testing
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

// Dependency helper plugins
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.4.0")
