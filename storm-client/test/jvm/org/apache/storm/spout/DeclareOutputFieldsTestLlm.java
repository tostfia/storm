package org.apache.storm.spout;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DeclareOutputFieldsTestLlm {
    @Test
    public void testDeclareOutputFieldsDeclaresCheckpointStream() {
        CheckpointSpout spout = new CheckpointSpout();
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        spout.declareOutputFields(declarer);
        verify(declarer).declareStream(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                argThat(fields -> fields.contains(CheckpointSpout.CHECKPOINT_FIELD_TXID)
                        && fields.contains(CheckpointSpout.CHECKPOINT_FIELD_ACTION))
        );
    }
}
