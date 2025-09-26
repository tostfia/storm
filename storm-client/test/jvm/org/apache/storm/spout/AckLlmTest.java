package org.apache.storm.spout;



import static org.mockito.Mockito.*;


import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;

import org.junit.Before;
import org.junit.Test;

public class AckLlmTest {

    private CheckpointSpout spout;
    private KeyValueState<String, CheckPointState> state;

    @Before
    public void setup() {
        spout = new CheckpointSpout();
        state = mock(KeyValueState.class);
        CheckPointState initialState = new CheckPointState(42L, CheckPointState.State.PREPARING);
        when(state.get("__state")).thenReturn(initialState);
        spout.open(mock(TopologyContext.class), mock(SpoutOutputCollector.class), 1000, state);
    }

    @Test
    public void testAckDuringRecoveryWithMatchingTxid() {
        spout.ack(42L);
        verify(state, atLeastOnce()).put(eq("__state"), any(CheckPointState.class));
        verify(state, atLeastOnce()).commit();
    }

    @Test
    public void testAckWithMismatchedTxid() {
        spout.ack(99L); // txid doesn't match
        verify(state, never()).put(eq("__state"), any(CheckPointState.class));
    }

    @Test(expected = NullPointerException.class)
    public void testAckWithNullMsgId() {
        spout.ack(null); // ci aspettiamo che lanci NullPointerException
    }

    @Test(expected = ClassCastException.class)
    public void testAckWithNonNumericMsgId() {
        spout.ack("invalid"); // should throw ClassCastException
    }
}