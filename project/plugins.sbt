// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

// Scalariform - Scala code formatting
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")

// Git Commands - Access to Git repo information
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")

// Static code analysis
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Test Coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
addSbtPlugin("com.github.mwz" % "sbt-sonar" % "2.2.0")

// Multi-JVM testing
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

// Dependency helper plugins
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
