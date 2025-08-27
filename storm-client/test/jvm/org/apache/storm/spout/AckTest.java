package org.apache.storm.spout;

import com.esotericsoftware.kryo.util.Null;
import org.apache.storm.task.TopologyContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class AckTest {

    @Parameterized.Parameter(0)
    public Object param;

    @Parameterized.Parameter(1)
    public Class<? extends Throwable> expectedResult;

    @Parameterized.Parameter(2)
    public String description;

    private CheckpointSpout spout;
    @Mock
    private TopologyContext context;
    @Mock private SpoutOutputCollector collector;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        spout = new CheckpointSpout();
        spout.open(new HashMap<>(), context, collector); // Assicura stato aperto
    }

    @After
    public void tearDown() { spout = null; }


    @Test
    public void testAck() {
        try {
            spout.ack(param);
            assertNull(expectedResult);
        } catch (ClassCastException e) {
            assertEquals(ClassCastException.class, expectedResult);
        } catch (NullPointerException e) {
            assertEquals(NullPointerException.class, expectedResult);
        }
    }

    @Parameterized.Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Tipi numerici validi
                {123L, null, "ack - Long valido"},
                {0L, null, "ack - Long zero"},
                {-10L, null, "ack - Long negativo"},
                {123.45, null, "ack - Double valido"},
                {0.0, null, "ack - Double zero"},
                {-1.23, null, "ack - Double negativo"},
                {42, null, "ack - Integer valido"},
                {(short)5, null, "ack - Short valido"},
                {(byte)1, null, "ack - Byte valido"},
                {3.14f, null, "ack - Float valido"},

                // Tipi non numerici
                {"Test", ClassCastException.class, "ack - Stringa"},
                {true, ClassCastException.class, "ack - Boolean"},
                {'A', ClassCastException.class, "ack - Character"},
                {new Object(), ClassCastException.class, "ack - Oggetto generico"},

                // Null
                {null, NullPointerException.class, "ack - msgId null"},

                // Array o collezioni
                {new Long[]{1L,2L}, ClassCastException.class, "ack - Array di Long"},
                {new int[]{1,2}, ClassCastException.class, "ack - Array di int"},
                {Arrays.asList(1L,2L), ClassCastException.class, "ack - List di Long"},

                // Oggetto custom
                {new CustomMsgId(), ClassCastException.class, "ack - Custom object non compatibile"}
        });
    }

    static class CustomMsgId { }


}

