// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

// TODO Add custom task to update the version variable in the docs project
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
      pushChanges
    )
  )
}
