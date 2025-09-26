package org.apache.storm.spout;


import org.apache.storm.tuple.Tuple;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IsCheckpointLlmTest {
    @Test
    public void testIsCheckpointWithValidCheckpointStream() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        assertTrue(CheckpointSpout.isCheckpoint(tuple));
    }

    @Test
    public void testIsCheckpointWithEmptyStreamId() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceStreamId()).thenReturn("");
        assertFalse(CheckpointSpout.isCheckpoint(tuple));
    }

    @Test
    public void testIsCheckpointWithNullStreamId() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceStreamId()).thenReturn(null);
        assertFalse(CheckpointSpout.isCheckpoint(tuple));
    }

    @Test
    public void testIsCheckpointWithSimilarStreamId() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceStreamId()).thenReturn("$checkpoints");
        assertFalse(CheckpointSpout.isCheckpoint(tuple));
    }
}
