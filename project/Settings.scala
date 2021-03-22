// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

import com.typesafe.sbt.SbtScalariform.autoImport.scalariformPreferences
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt.Keys._
import sbt._
import scalariform.formatter.preferences._

object Settings extends AutoPlugin {
  object autoImport {
  }
  private val headerSettings = Seq(
    headerLicense := Some(HeaderLicense.Custom("Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>")),

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
  )

  val gearsToolsMavenRelease = "gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/"

  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = headerSettings ++ scalariformSettings ++ Seq(
    publishTo := {
      if (isSnapshot.value) {
        Some("artifactory.mia.ulti.io-gt-snapshots" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-snapshot")
      } else {
        Some("gears-tools-maven-release" at "https://artifactory.mia.ulti.io/artifactory/gt-maven-libs-release/")
      }
    },
    publishMavenStyle := true,
    Test / parallelExecution := false
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    organization in ThisBuild := "com.ukg",

    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-unchecked",
      "-deprecation",
      "-feature"
    )
  )
}
