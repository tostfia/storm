package org.apache.storm.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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

@RunWith(Parameterized.class)
public class DeclareOutputFieldsTest {

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
        context = mock(TopologyContext.class);
        collector = mock(SpoutOutputCollector.class);
        spout = new CheckpointSpout();
        spout.open(new HashMap<>(), context, collector); // Assicura stato aperto
    }

    @After
    public void tearDown() {
        spout = null;
    }

    @Test
    public void testDeclareOutputFields() {
        try {
            spout.declareOutputFields((OutputFieldsDeclarer) param);
            assertNull("Nessuna eccezione attesa", expectedResult);
        } catch (Throwable e) {
            assertEquals("Eccezione attesa non corrisponde", expectedResult, e.getClass());
        }
    }

    @Parameterized.Parameters(name = "{2}")
    public static Object[][] data() {
        OutputFieldsDeclarer validDeclarer = mock(OutputFieldsDeclarer.class);

        return new Object[][]{
                {validDeclarer, null, "declareOutputFields - declarer valido"},
                {null, NullPointerException.class, "declareOutputFields - declarer null"},
                {"InvalidType", ClassCastException.class, "declareOutputFields - input String"},
                {123, ClassCastException.class, "declareOutputFields - input Integer"},
                {3.14, ClassCastException.class, "declareOutputFields - input Double"},
                {new Object(), ClassCastException.class, "declareOutputFields - oggetto generico"},
        };
    }

}
