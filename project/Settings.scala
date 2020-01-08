// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

import com.typesafe.sbt.SbtScalariform.autoImport.scalariformPreferences
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt._
import sbt.Keys._
import scalariform.formatter.preferences._

object Settings extends AutoPlugin {
  object autoImport {
  }
  private val headerSettings = Seq(
    headerLicense := Some(HeaderLicense.Custom("Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>")),

    headerMappings := headerMappings.value ++ Seq(
      HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.java -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType("kt") -> HeaderCommentStyle.cppStyleLineComment
    )
  )

  private val scalariformSettings = Seq(
    scalariformPreferences := scalariformPreferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentConstructorArguments, true)
      .setPreference(RewriteArrowSymbols, true)
  )

  val gearsToolsMavenRelease = "gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/"
  private val nuLocalArtifactory = "ucartifactory.mia.ucloud.int" at "https://artifactory.mia.ulti.io/artifactory/ultimate-nu-local"
  // Mulesoft resolver needed for org.everit.json.schema:1.9.2, which is pulled in by one of the mp libraries
  private val mulesoft = "multsoft" at "https://repository.mulesoft.org/nexus/content/repositories/public/"

  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = headerSettings ++ scalariformSettings ++ Seq(
    publishTo := {
      if (isSnapshot.value) {
        Some("artifactory.mia.ulti.io-gt-snapshots" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-plugins-snapshot")
      } else {
        Some(gearsToolsMavenRelease)
      }
    },
    publishMavenStyle := true,
    Test / parallelExecution := false
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    organization in ThisBuild := "com.ultimatesoftware",

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature"
    ),

    resolvers ++= Seq(gearsToolsMavenRelease, nuLocalArtifactory, mulesoft)
  )
}
