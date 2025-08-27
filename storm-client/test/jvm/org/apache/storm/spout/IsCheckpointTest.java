package org.apache.storm.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class IsCheckpointTest {

    @Parameterized.Parameter(0)
    public Object param;

    @Parameterized.Parameter(1)
    public Class<? extends Throwable> expectedResult;

    @Parameterized.Parameter(2)
    public String description;

    private CheckpointSpout spout;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        spout = new CheckpointSpout();
        context = mock(TopologyContext.class);
        collector = mock(SpoutOutputCollector.class);
        spout.open(new HashMap<>(), context, collector);
    }

    @After
    public void tearDown() {
        spout = null;
    }

    @Test
    public void testIsCheckpoint() {
        try {
            spout.isCheckpoint((Tuple) param);
            assertNull("Nessuna eccezione attesa", expectedResult);
        } catch (Throwable e) {
            assertEquals("Eccezione attesa non corrisponde", expectedResult, e.getClass());
        }
    }

    @Parameterized.Parameters(name = "{2}")
    public static Object[][] data() {
        // Tuple validi
        Tuple checkpointTuple = mock(Tuple.class);
        when(checkpointTuple.getValueByField("checkpoint")).thenReturn(true);

        Tuple normalTuple = mock(Tuple.class);
        when(normalTuple.getValueByField("checkpoint")).thenReturn(false);

        Tuple tupleWithNullField = mock(Tuple.class);
        when(tupleWithNullField.getValueByField("checkpoint")).thenReturn(null);

        return new Object[][] {
                {checkpointTuple, null, "isCheckpoint - tuple valida checkpoint"},
                {normalTuple, null, "isCheckpoint - tuple valida non checkpoint"},
                {tupleWithNullField, null, "isCheckpoint - tuple con campo checkpoint null"},
                {null, NullPointerException.class, "isCheckpoint - tuple null"},
                {"NonTuple", ClassCastException.class, "isCheckpoint - input String (non Tuple)"},
                {123, ClassCastException.class, "isCheckpoint - input Integer (non Tuple)"},
                {3.14, ClassCastException.class, "isCheckpoint - input Double (non Tuple)"},
                {new Object(), ClassCastException.class, "isCheckpoint - oggetto generico"}
        };
    }
}
