// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream

trait ReleasableStream {
  def release(): Unit
}
