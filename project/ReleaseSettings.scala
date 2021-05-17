// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

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
      // Push once here to trigger a GH Actions workflow for the new version since if we push
      // this commit and the new snapshot version it just picks up the latest commit
      pushChanges,
      setNextVersion,
      commitNextVersion,
      pushChanges))
}
