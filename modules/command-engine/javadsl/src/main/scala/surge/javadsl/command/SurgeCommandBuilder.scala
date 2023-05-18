// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core.commondsl.SurgeCommandBusinessLogicTrait
import scala.concurrent.ExecutionContext
import surge.internal.utils.DiagnosticContextFuturePropagation

class Buildable[AggId, Agg, Command, Evt](businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt]) {
  def build(): SurgeCommand[AggId, Agg, Command, Evt] = {
    SurgeCommand.create(businessLogic, DiagnosticContextFuturePropagation.global)
  }
}

class SurgeCommandBuilder {

  def withBusinessLogic[AggId, Agg, Command, Evt](
      businessLogic: SurgeCommandBusinessLogicTrait[AggId, Agg, Command, Evt]): Buildable[AggId, Agg, Command, Evt] = {
    new Buildable[AggId, Agg, Command, Evt](businessLogic)
  }

}
