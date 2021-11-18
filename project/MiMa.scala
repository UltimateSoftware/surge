// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

import com.typesafe.tools.mima.core._
import sbt._
import sbt.Keys._
import com.typesafe.tools.mima.plugin.MimaPlugin
import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport._

object MiMa extends AutoPlugin {

  override def requires: Plugins = MimaPlugin
  override def trigger: PluginTrigger = allRequirements

  override val projectSettings = Seq(
    mimaReportSignatureProblems := true,
    mimaPreviousArtifacts := previousArtifacts(name.value, organization.value),
    mimaExcludeAnnotations := Seq("surge.annotations.Experimental", "surge.annotations.ApiMayChange", "surge.annotations.InternalApi"),
    mimaBinaryIssueFilters ++= Seq(ProblemFilters.exclude[Problem]("surge.internal.*")))

  private def previousArtifacts(projectName: String, organization: String): Set[sbt.ModuleID] = {
    val versions: Seq[String] = {
      val firstPatchVersion = "19"
      Seq(s"0.5.$firstPatchVersion")
    }

    versions.map { v =>
      organization %% projectName % v
    }.toSet
  }
}
