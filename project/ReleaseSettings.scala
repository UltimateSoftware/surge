// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

// TODO Rework this process for open source...

object ReleaseSettings {
  // The actual artifacts are published via Github Actions when a tag is created
  val settings = Seq(
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      setNextVersion,
      commitNextVersion,
      pushChanges))
}
