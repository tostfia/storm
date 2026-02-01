package org.apache.storm.spout;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.apache.storm.task.TopologyContext;



import java.util.concurrent.atomic.AtomicBoolean;

public class CheckpointSpoutFullTest {

    private CheckpointSpout spout;
    private InMemoryKeyValueState<String, CheckPointState> state;

    @Mock
    private SpoutOutputCollector collector;

    @Mock
    private TopologyContext context;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        collector = mock(SpoutOutputCollector.class);
        context = mock(TopologyContext.class);

        state = new InMemoryKeyValueState<>();
        spout = new CheckpointSpout();




    }

    @Test
    public void testFullFlow() {

        // Stato iniziale: PREPARING → simula ciclo completo
        state.put("__state", new CheckPointState(1L, CheckPointState.State.PREPARING));

        spout.open(this.context, this.collector, 1000, state);

        // -------------------------
        // 1. Ramo PREPARING → emette ROLLBACK
        // -------------------------
        AtomicBoolean emittedRollback = new AtomicBoolean(false);
        doAnswer(invocation -> {
            emittedRollback.set(true);
            return null;
        }).when(collector).emit(eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                eq(new Values(1L, CheckPointState.Action.ROLLBACK)),
                eq(1L));

        spout.nextTuple();
        assertTrue(emittedRollback.get());

        // -------------------------
        // 2. Ack sul PREPARING → INITSTATE
        // -------------------------
        AtomicBoolean emittedInitState = new AtomicBoolean(false);
        doAnswer(invocation -> {
            emittedInitState.set(true);
            return null;
        }).when(collector).emit(eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                eq(new Values(0L, CheckPointState.Action.INITSTATE)),
                eq(0L));

        spout.ack(1L);
        spout.nextTuple();
        assertTrue(emittedInitState.get());

        // -------------------------
        // 3. Ack INITSTATE → PREPARE
        // -------------------------
        AtomicBoolean emittedPrepare = new AtomicBoolean(false);
        doAnswer(invocation -> {
            emittedPrepare.set(true);
            return null;
        }).when(collector).emit(eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                eq(new Values(1L, CheckPointState.Action.PREPARE)),
                eq(1L));

        spout.ack(0L);
        spout.nextTuple();
        assertTrue(emittedPrepare.get());

        // -------------------------
        // 4. Ack PREPARE → COMMIT
        // -------------------------
        AtomicBoolean emittedCommit = new AtomicBoolean(false);
        doAnswer(invocation -> {
            emittedCommit.set(true);
            return null;
        }).when(collector).emit(eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                eq(new Values(1L, CheckPointState.Action.COMMIT)),
                eq(1L));

        spout.ack(1L);
        spout.nextTuple();
        assertTrue(emittedCommit.get());

        // -------------------------
        // 5. Fail su COMMIT → imposta recovering = true
        // -------------------------
        spout.fail(1L);

        // -------------------------
        // 6. Ack con recovering = true → handleRecoveryAck
        // -------------------------
        AtomicBoolean emittedRecovery = new AtomicBoolean(false);
        doAnswer(invocation -> {
            emittedRecovery.set(true);
            return null;
        }).when(collector).emit(eq(CheckpointSpout.CHECKPOINT_STREAM_ID), anyList(), any());

        spout.ack(1L);
        spout.nextTuple();
        assertTrue(emittedRecovery.get());
    }

    @Test
    public void testSleepIntervalDerivedFromCheckpointInterval() throws Exception {
        int checkpointInterval = 2000;

        // Stato iniziale valido
        state.put("__state", new CheckPointState(1L, CheckPointState.State.COMMITTED));

        // open() è il punto dove sleepInterval viene calcolato
        spout.open(this.context, this.collector, checkpointInterval, state);

        // Osservazione white-box dello stato interno
        java.lang.reflect.Field f =
                CheckpointSpout.class.getDeclaredField("sleepInterval");
        f.setAccessible(true);

        int sleepInterval = (int) f.get(spout);

        // Contratto funzionale:
        // sleepInterval deve essere proporzionale e MOLTO più piccolo del checkpointInterval
        assertEquals(checkpointInterval / 10, sleepInterval);
    }


}

