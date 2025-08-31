package org.apache.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.task.TopologyContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class NextTupleTest {
    private CheckpointSpout spout;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    private InMemoryKeyValueState<String, CheckPointState> state;

    @Before
    public void setup() {
        collector = mock(SpoutOutputCollector.class);
        context = mock(TopologyContext.class);
        state = new InMemoryKeyValueState<>();

        // Stato iniziale con txid -1 e COMMITTED
        state.put("__state", new CheckPointState(-1, CheckPointState.State.COMMITTED));

        spout = new CheckpointSpout();
        spout.open(context, collector, 100, state);
    }

    @Test
    public void testSleepBranch() {
        spout.nextTuple();
        verify(collector, atLeastOnce()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );

    }
}
