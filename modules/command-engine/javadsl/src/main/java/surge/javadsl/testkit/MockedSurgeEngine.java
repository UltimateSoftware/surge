// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.testkit;

import org.mockito.Mockito;
import surge.javadsl.command.SurgeCommand;

public class MockedSurgeEngine<AggId, Agg, Command, Event> {

    private SurgeCommand<AggId, Agg, Command, Event> mockSurgeEngine;

    public MockedSurgeEngine() {
        // empty constructor
        mockSurgeEngine = Mockito.mock(SurgeCommand.class);
    }

    /**
     * Constructor for users who want to bring in their own mocked surge engine
     *
     * @param mockSurgeEngine
     */
    public MockedSurgeEngine(SurgeCommand<AggId, Agg, Command, Event> mockSurgeEngine) {
        this.mockSurgeEngine = mockSurgeEngine;
    }

    /**
     * For users who want to fetch the underlying mock and do something else with it
     */
    public SurgeCommand<AggId, Agg, Command, Event> get() {
        return mockSurgeEngine;
    }


    /**
     * Fluent setter
     *
     * @param mockSurgeEngine
     */
    public void withMockedSurgeEngine(SurgeCommand<AggId, Agg, Command, Event> mockSurgeEngine) {
        this.mockSurgeEngine = mockSurgeEngine;
    }

    public WhenSendCommand<AggId, Agg, Command, Event> whenSendCommand(AggId aggId, Command command) {
        return new WhenSendCommand<AggId, Agg, Command, Event>(mockSurgeEngine, aggId, command);
    }

    public WhenGetAggregate<AggId, Agg, Command, Event> whenGetAggregate(AggId aggId) {
        return new WhenGetAggregate<AggId, Agg, Command, Event>(mockSurgeEngine, aggId);
    }

}
