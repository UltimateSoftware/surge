// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package org.apache.kafka.streams

// Create this here since we create instances of this simple object for various tests, but the constructor
// for LagInfo is not public
class MockLagInfo(currentOffset: Long, endOffset: Long) extends LagInfo(currentOffset, endOffset)
