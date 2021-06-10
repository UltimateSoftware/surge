// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

import com.typesafe.sbt.SbtGit.git
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt.Keys._
import sbt._

object Settings extends AutoPlugin {
  object autoImport {}
  private val headerSettings = Seq(
    headerLicense := Some(HeaderLicense.Custom("Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>")),
    headerMappings := headerMappings.value ++ Seq(
      HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType.java -> HeaderCommentStyle.cppStyleLineComment,
      HeaderFileType("kt") -> HeaderCommentStyle.cppStyleLineComment))

  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = headerSettings ++ Seq(
    // TODO figure out publishing
    publishMavenStyle := true,
    scmInfo := Some(ScmInfo(url("https://github.com/UltimateSoftware/surge"), "scm:git:git@github.com:UltimateSoftware/surge.git")),
    git.remoteRepo := scmInfo.value.get.connection.replace("scm:git:", ""),
    Test / parallelExecution := false)

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    organization in ThisBuild := "com.ukg",
    scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature"),
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD", "-u", "target/test-reports"))
}
