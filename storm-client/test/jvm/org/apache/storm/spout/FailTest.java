package org.apache.storm.spout;

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
public class FailTest {

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
    public void tearDown() {
        spout = null;
    }

    @Test
    public void testFail() {
        try{
            spout.fail(param);
            assertNull(expectedResult);
        }catch (ClassCastException e) {
            assertEquals(ClassCastException.class, expectedResult);
        } catch (NullPointerException e) {
            assertEquals(NullPointerException.class, expectedResult);
        }
    }

    @Parameterized.Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Tipi numerici validi
                {123L, null, "fail - Long valido"},
                {0L, null, "fail - Long zero"},
                {-10L, null, "fail - Long negativo"},
                {123.45, null, "fail - Double valido"},
                {0.0, null, "fail - Double zero"},
                {-1.23, null, "fail - Double negativo"},
                {42, null, "fail - Integer valido"},
                {(short)5, null, "fail - Short valido"},
                {(byte)1, null, "fail - Byte valido"},
                {3.14f, null, "fail - Float valido"},

                // Tipi non numerici
                {"Test",null, "fail - Stringa"},
                {true, null, "fail - Boolean"},
                {'A', null, "fail - Character"},
                {new Object(), null, "fail- Oggetto generico"},

                // Null
                {null,null, "fail - msgId null"},

                // Array o collezioni
                {new Long[]{1L,2L}, null, "fail - Array di Long"},
                {new int[]{1,2},null, "fail - Array di int"},
                {Arrays.asList(1L,2L),null, "ack - List di Long"},

                // Oggetto custom
                {new CustomMsgId(), null, "fail - Custom object non compatibile"}
        });
    }

    static class CustomMsgId { }
}
