// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait

class Buildable[AggId, Agg, Command, Evt, Response](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt, Response]) {
  def build(): SurgeCommand[AggId, Agg, Command, Nothing, Evt, Response] = {
    SurgeCommand.create(businessLogic)
  }
}

class SurgeCommandBuilder {

  def withBusinessLogic[AggId, Agg, Command, Evt, Response](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt, Response]): Buildable[AggId, Agg, Command, Evt, Response] = {
    new Buildable[AggId, Agg, Command, Evt, Response](businessLogic)
  }

}
