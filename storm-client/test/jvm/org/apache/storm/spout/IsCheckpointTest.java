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
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class IsCheckpointTest {

    @Parameterized.Parameter(0)
    public Object param;

    @Parameterized.Parameter(1)
    public Class<? extends Throwable> expectedException; // Rinomina per chiarezza: ora gestisce solo le eccezioni

    @Parameterized.Parameter(2)
    public Boolean expectedBooleanReturn; // NUOVO: Valore booleano atteso dal metodo isCheckpoint()

    @Parameterized.Parameter(3)
    public String description; // Indice spostato

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
        if (expectedException != null) {
            // Caso in cui ci si aspetta un'eccezione
            try {
                CheckpointSpout.isCheckpoint((Tuple) param);
                fail("Prevista eccezione " + expectedException.getSimpleName() + " ma nessuna eccezione è stata lanciata per: " + description);
            } catch (Throwable e) {
                assertEquals("Il tipo di eccezione attesa non corrisponde per: " + description, expectedException, e.getClass());
            }
        } else {
            // Caso in cui non ci si aspetta un'eccezione, quindi si verifica il valore booleano di ritorno
            assertNotNull("Il valore booleano di ritorno atteso non può essere null quando non è prevista alcuna eccezione per: " + description, expectedBooleanReturn);
            boolean actualResult = CheckpointSpout.isCheckpoint((Tuple) param);
            assertEquals("Il valore di ritorno non corrisponde per: " + description, expectedBooleanReturn, actualResult);
        }
    }

    @Parameterized.Parameters(name = "{3}") // L'indice della descrizione è ora 3
    public static Object[][] data() {
        // Tuple valide
        Tuple checkpointTuple = mock(Tuple.class);
        when(checkpointTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);

        Tuple normalTuple = mock(Tuple.class);
        when(normalTuple.getSourceStreamId()).thenReturn("some-other-stream-id"); // Un ID stream qualsiasi non di checkpoint

        Tuple tupleWithNullStreamId = mock(Tuple.class);
        when(tupleWithNullStreamId.getSourceStreamId()).thenReturn(null);

        return new Object[][] {
                // param, expectedException, expectedBooleanReturn, description
                {checkpointTuple, null, true, "isCheckpoint - tuple valida checkpoint"},
                {normalTuple, null, false, "isCheckpoint - tuple valida non checkpoint"},
                {tupleWithNullStreamId, null, false, "isCheckpoint - tuple con campo checkpoint null"}, // Assumendo che un campo null non sia un checkpoint
                {null, NullPointerException.class, null, "isCheckpoint - tuple null"}, // Nessun valore booleano atteso per i casi di eccezione
                {"NonTuple", ClassCastException.class, null, "isCheckpoint - input String (non Tuple)"},
                {123, ClassCastException.class, null, "isCheckpoint - input Integer (non Tuple)"},
                {3.14, ClassCastException.class, null, "isCheckpoint - input Double (non Tuple)"},
                {new Object(), ClassCastException.class, null, "isCheckpoint - oggetto generico"}
        };
    }
}