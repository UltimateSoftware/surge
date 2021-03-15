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
    mimaBinaryIssueFilters ++= Seq(
      ProblemFilters.exclude[Problem]("surge.internal.*")
    )
  )

  // TODO When we release and actually enable this, we'll need to set the patch version to 0
  //  For now we can just experiment with and get used to the plugin
  private def previousArtifacts(projectName: String, organization: String): Set[sbt.ModuleID] = {
    val versions: Seq[String] = {
      val firstPatchVersion = "0-RC12"
      Seq(s"0.5.$firstPatchVersion")
    }

    versions.map { v =>
      organization %% projectName % v
    }.toSet
  }
}
