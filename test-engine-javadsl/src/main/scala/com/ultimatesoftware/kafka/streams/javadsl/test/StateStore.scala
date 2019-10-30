// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

import scala.collection.mutable

class StateStore[AggId, Agg] {
  private val stateMap: mutable.Map[AggId, Agg] = mutable.Map.empty

  def getState(aggId: AggId): Option[Agg] = {
    stateMap.get(aggId)
  }

  def putState(aggId: AggId, agg: Agg): Unit = {
    stateMap.put(aggId, agg)
  }

  def putState(aggId: AggId, aggOpt: Option[Agg]): Unit = {
    aggOpt match {
      case Some(agg) ⇒ putState(aggId, agg)
      case None      ⇒ stateMap.remove(aggId)
    }
  }
}
