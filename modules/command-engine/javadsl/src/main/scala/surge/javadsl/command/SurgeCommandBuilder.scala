// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import scala.concurrent.ExecutionContext

class Buildable[AggId, Agg, Command, Evt](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt], ec: ExecutionContext) {
  def build(): SurgeCommand[AggId, Agg, Command, Evt] = {
    SurgeCommand.create(businessLogic)(ec)
  }
}

class SurgeCommandBuilder {

  def withBusinessLogic[AggId, Agg, Command, Evt](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt],
      ec: ExecutionContext): Buildable[AggId, Agg, Command, Evt] = {
    new Buildable[AggId, Agg, Command, Evt](businessLogic, ec)
  }

}
