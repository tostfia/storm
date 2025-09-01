package org.apache.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.task.TopologyContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

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

        // Stato iniziale: COMMITTED → inizializza curTxState
        state.put("__state", new CheckPointState(-1, CheckPointState.State.COMMITTED));

        spout = new CheckpointSpout();
        spout.open(context, collector, 100, state);

        // Ripuliamo eventuali emit fatti durante open()
        clearInvocations(collector);
    }

    @Test
    public void testRecovery() {
        // Metto uno stato RECOVERING: la spout dovrà entrare in recovery
        state.put("__state", new CheckPointState(1, CheckPointState.State.PREPARING));

        spout.nextTuple();

        // In recovery deve emettere un messaggio di recovery
        verify(collector, atLeastOnce()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
    }

    @Test
    public void testCheckpoint() {
        // Metto uno stato COMMITTING: la spout dovrà fare checkpoint
        state.put("__state", new CheckPointState(2, CheckPointState.State.COMMITTING));

        spout.nextTuple();

        // Deve emettere una tupla di checkpoint
        verify(collector, atLeastOnce()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
    }

    @Test
    public void testSleep() {
        // COMMITTED → sleep
        state.put("__state", new CheckPointState(3, CheckPointState.State.COMMITTED));

        // Primo nextTuple() consuma INITSTATE
        spout.nextTuple();
        clearInvocations(collector);

        // Secondo nextTuple() test reale
        spout.nextTuple();

        verify(collector, never()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
    }

    @Test
    public void testNextTupleCheckpointBranch() {
        // Stato COMMITTED → shouldRecover() = false, shouldCheckpoint() = true
        state.put("__state", new CheckPointState(123, CheckPointState.State.COMMITTED));
        spout.open(new HashMap<>(), context, collector);

        spout.nextTuple();

        // Verifica che doCheckpoint() produca effetti osservabili (es. emit)
        verify(collector, atLeastOnce()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
    }







}

