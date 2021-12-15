package surge.javadsl.testkit;


import org.mockito.Mockito;
import surge.javadsl.command.AggregateRef;
import surge.javadsl.command.SurgeCommand;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Mockito.when;

public class WhenGetAggregate<AggId, Agg, Command, Event> {

    private SurgeCommand<AggId, Agg, Command, Event> mockSurgeEngine;

    private AggId aggId;

    public WhenGetAggregate(SurgeCommand<AggId, Agg, Command, Event> mock, AggId aggId) {
        this.mockSurgeEngine = mock;
        this.aggId = aggId;
    }

    public void thenReturn(Optional<Agg> optionalAgg) throws Exception {
        AggregateRef<Agg, Command, Event> mockAggregate = Mockito.mock(AggregateRef.class);
        CompletionStage<Optional<Agg>> mockCompletionStage = Mockito.mock(CompletionStage.class);
        CompletableFuture<Optional<Agg>> mockCompletableFuture = Mockito.mock(CompletableFuture.class);

        when(mockSurgeEngine.aggregateFor(aggId)).thenReturn(mockAggregate);
        when(mockAggregate.getState()).thenReturn(mockCompletionStage);
        when(mockCompletionStage.toCompletableFuture()).thenReturn(mockCompletableFuture);
        when(mockCompletableFuture.get()).thenReturn(optionalAgg);
    }

    public void returnAggregate(Agg agg) throws Exception {
        thenReturn(Optional.of(agg));
    }

    public void noSuchAggregate() throws Exception {
        thenReturn(Optional.empty());
    }

}
