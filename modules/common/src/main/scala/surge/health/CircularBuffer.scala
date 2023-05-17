// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health

import java.util.concurrent.locks.ReentrantReadWriteLock

class CircularBuffer[T](size: Int)(implicit mf: Manifest[T]) {

  private val arr = new scala.collection.mutable.ArrayBuffer[T]()

  private var cursor = 0

  val monitor = new ReentrantReadWriteLock()

  def push(value: T): Unit = {
    monitor.writeLock().lock()
    try {
      if (cursor == size) {
        cursor = 0
        arr.update(cursor, value)
        cursor += 1
      } else {
        cursor += 1
        arr.append(value)
      }
    } finally {
      monitor.writeLock().unlock()
    }
  }

  def getAll: Array[T] = {
    monitor.readLock().lock()
    try {
      val copy = new Array[T](size)
      arr.copyToArray(copy)
      copy.toSeq.filter(i => i != null).toArray
    } finally {
      monitor.readLock().unlock()
    }
  }

  def clear(): Unit = {
    monitor.writeLock().lock()
    try {
      arr.clear()
      cursor = 0
    } finally {
      monitor.writeLock().unlock()
    }
  }
}
