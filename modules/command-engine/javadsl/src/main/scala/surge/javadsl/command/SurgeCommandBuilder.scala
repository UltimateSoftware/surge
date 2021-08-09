// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait

class Buildable[AggId, Agg, Command, Evt](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt]) {
  def build(): SurgeCommand[AggId, Agg, Command, Nothing, Evt] = {
    SurgeCommand.create(businessLogic)
  }
}

class SurgeCommandBuilder {
  def withBusinessLogic[AggId, Agg, Command, Evt](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt]): Buildable[AggId, Agg, Command, Evt] = {
    new Buildable[AggId, Agg, Command, Evt](businessLogic)
  }
}
