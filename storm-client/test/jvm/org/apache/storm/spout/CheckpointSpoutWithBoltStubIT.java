package org.apache.storm.spout;

import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.spout.CheckPointState.Action;
import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CheckpointSpoutWithBoltStubIT {

    private CheckpointSpout spout;
    private SpoutCollectorStub collectorStub;
    private KeyValueState<String, CheckPointState> stateStub;

    @Before
    public void setup() {
        collectorStub = new SpoutCollectorStub();
        stateStub = new InMemoryKeyValueState<>();

        // Inizializza lo stato con uno stato NON COMMITTED
        stateStub.put("__state", new CheckPointState(0, CheckPointState.State.PREPARING));

        spout = new CheckpointSpout();
        TopologyContext ctx = mock(TopologyContext.class);
        spout.open(ctx, collectorStub, 100, stateStub);
    }


    @Test
    public void testFirstRecoveryEmit() {
        spout.nextTuple();

        assertEquals("Dovrebbe esserci un'emissione durante recovery", 1, collectorStub.emitted.size());
        Values values = collectorStub.emitted.get(0).values;

        assertTrue(values.get(0) instanceof Long);
        assertTrue(values.get(1) instanceof Action);

        System.out.println("Emesso in recovery: " + values);
    }

    @Test
    public void testRecoveryAckProgress() {


        spout.nextTuple(); // emette la recovery action
        Values values = collectorStub.emitted.get(0).values;
        Long txid = (Long) values.get(0);

        spout.ack(txid); // ack del messaggio

        CheckPointState newState = stateStub.get("__state");
        assertNotNull(newState);
        assertNotEquals("Lo stato deve avanzare rispetto a PREPARING", CheckPointState.State.PREPARING, newState.getState());

    }


    @Test
    public void testFailTriggersRecovery() {
        spout.nextTuple();
        Values values = collectorStub.emitted.get(0).values;
        Long txid = (Long) values.get(0);

        // simuliamo un fail
        spout.fail(txid);

        // dopo fail la spout resta in recovery
        assertNotNull("La spout deve essere in recovery", stateStub.get("__state"));
    }

    // -------------------------------
    // STUB COLLECTOR & STATE
    // -------------------------------

    private static class SpoutCollectorStub extends org.apache.storm.spout.SpoutOutputCollector {
        List<EmitRecord> emitted = new ArrayList<>();

        SpoutCollectorStub() { super(null); }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            emitted.add(new EmitRecord(streamId, new Values(tuple.toArray()), messageId));
            return Collections.singletonList(0);
        }
    }

    private static class EmitRecord {
        String streamId;
        Values values;
        Object msgId;

        EmitRecord(String streamId, Values values, Object msgId) {
            this.streamId = streamId;
            this.values = values;
            this.msgId = msgId;
        }
    }


}
