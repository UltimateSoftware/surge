// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.testkit;

import org.mockito.Mockito;
import surge.javadsl.command.AggregateRef;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.common.CommandResult;
import surge.javadsl.common.CommandSuccess;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Mockito.when;

public class WhenSendCommand<AggId, Agg, Command, Rej, Event> {

    private SurgeCommand<AggId, Agg, Command, Rej, Event> mockSurgeEngine;

    private AggId aggId;

    private Command command;

    public WhenSendCommand(SurgeCommand<AggId, Agg, Command, Rej, Event> mock, AggId aggId, Command command) {
        this.mockSurgeEngine = mock;
        this.aggId = aggId;
        this.command = command;
    }

    public void thenReturn(Optional<Agg> optionalAgg) throws Exception {
        AggregateRef<Agg, Command, Event> mockAggregate = Mockito.mock(AggregateRef.class);
        CompletionStage<CommandResult<Agg>> mockCompletionStage = Mockito.mock(CompletionStage.class);
        CompletableFuture<CommandResult<Agg>> mockCompletableFuture = Mockito.mock(CompletableFuture.class);
        CommandSuccess<Agg> mockCommandSuccess = Mockito.mock(CommandSuccess.class);

        when(mockSurgeEngine.aggregateFor(aggId)).thenReturn(mockAggregate);
        when(mockAggregate.sendCommand(command)).thenReturn(mockCompletionStage);
        when(mockCompletionStage.toCompletableFuture()).thenReturn(mockCompletableFuture);
        when(mockCompletableFuture.get()).thenReturn(mockCommandSuccess);
        when(mockCommandSuccess.aggregateState()).thenReturn(optionalAgg);
    }

    public void returnAggregate(Agg agg) throws Exception {
        thenReturn(Optional.of(agg));
    }

    public void noSuchAggregate() throws Exception {
        thenReturn(Optional.empty());
    }

}
