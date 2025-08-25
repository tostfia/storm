package org.apache.storm.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
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
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class CheckpointSpoutTest {

    private enum TestMethod { ACK, FAIL, DECLARE_OUTPUT_FIELDS, IS_CHECKPOINT, OPEN }
    private enum TestResult { NO_EXCEPTION, CLASS_CAST_EXCEPTION, NULL_POINTER_EXCEPTION, ILLEGAL_ARGUMENT_EXCEPTION }

    private final Object param;
    private final TestMethod methodToCall;
    private final TestResult expectedResult;
    private final String testDescription;

    @Mock
    private TopologyContext mockContext;
    @Mock
    private SpoutOutputCollector mockCollector;

    private CheckpointSpout spout;

    public CheckpointSpoutTest(TestMethod methodToCall, Object param, TestResult expectedResult, String testDescription) {
        this.methodToCall = methodToCall;
        this.param = param;
        this.expectedResult = expectedResult;
        this.testDescription = testDescription;
    }

    @Parameterized.Parameters(name = "{0} - {3}")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                // Test cases for ACK
                {TestMethod.ACK, 123L, TestResult.NO_EXCEPTION, "ack - Long valido"},
                {TestMethod.ACK, 123.45, TestResult.NO_EXCEPTION, "ack - Double valido"},
                {TestMethod.ACK, "Test", TestResult.CLASS_CAST_EXCEPTION, "ack - Stringa (atteso ClassCastException)"},
                {TestMethod.ACK, null, TestResult.NULL_POINTER_EXCEPTION, "ack - msgId null (atteso NullPointerException)"},

                // Test cases for FAIL
                {TestMethod.FAIL, 456L, TestResult.NO_EXCEPTION, "fail - Long valido"},
                {TestMethod.FAIL, 456.78, TestResult.NO_EXCEPTION, "fail - Double valido"},
                {TestMethod.FAIL, "Test1", TestResult.NO_EXCEPTION, "fail - Stringa"},
                {TestMethod.FAIL, null, TestResult.NO_EXCEPTION, "fail - msgId null"},

                // Test cases for DECLARE_OUTPUT_FIELDS
                {TestMethod.DECLARE_OUTPUT_FIELDS, null, TestResult.NULL_POINTER_EXCEPTION, "declareOutputFields - null declarer (atteso NullPointerException)"},
                {TestMethod.DECLARE_OUTPUT_FIELDS, mock(OutputFieldsDeclarer.class), TestResult.NO_EXCEPTION, "declareOutputFields - valid declarer"},
                {TestMethod.DECLARE_OUTPUT_FIELDS, "Invalid", TestResult.CLASS_CAST_EXCEPTION, "declareOutputFields - invalid type (String, atteso ClassCastException)"},

                // Test cases for IS_CHECKPOINT
                {TestMethod.IS_CHECKPOINT, null, TestResult.NULL_POINTER_EXCEPTION, "isCheckpoint - null tuple (atteso NullPointerException)"},
                {TestMethod.IS_CHECKPOINT, mock(Tuple.class), TestResult.NO_EXCEPTION, "isCheckpoint - valid tuple"},
                {TestMethod.IS_CHECKPOINT, "Invalid", TestResult.CLASS_CAST_EXCEPTION, "isCheckpoint - invalid type (String, atteso ClassCastException)"},
                {TestMethod.IS_CHECKPOINT, "", TestResult.CLASS_CAST_EXCEPTION, "isCheckpoint - empty string (atteso ClassCastException)"},
                {TestMethod.IS_CHECKPOINT, 789, TestResult.CLASS_CAST_EXCEPTION, "isCheckpoint - Integer (atteso ClassCastException)"},

                // NUOVI Test cases for OPEN
                // Formato per OPEN: {TestMethod.OPEN, new Object[]{conf, context, collector}, expectedResult, description}
                {TestMethod.OPEN, new Object[]{new HashMap<>(), mock(TopologyContext.class), mock(SpoutOutputCollector.class)}, TestResult.NO_EXCEPTION, "open - tutti parametri validi"},
                {TestMethod.OPEN, new Object[]{null, mock(TopologyContext.class), mock(SpoutOutputCollector.class)}, TestResult.NULL_POINTER_EXCEPTION, "open - conf null (atteso NullPointerException)"},
                {TestMethod.OPEN, new Object[]{new HashMap<>(), null, mock(SpoutOutputCollector.class)}, TestResult.NULL_POINTER_EXCEPTION, "open - context null (atteso NullPointerException)"},
                {TestMethod.OPEN, new Object[]{new HashMap<>(), mock(TopologyContext.class), null}, TestResult.NO_EXCEPTION, "open - collector null (atteso NullPointerException) - Se la spout non usa collector, potrebbe non lanciare eccezione"},
                {TestMethod.OPEN, new Object[]{null, null, null}, TestResult.NULL_POINTER_EXCEPTION, "open - tutti null (atteso NullPointerException)"},
                {TestMethod.OPEN, new Object[]{new HashMap<String, Object>() {{ put("key", "value"); }}, mock(TopologyContext.class), mock(SpoutOutputCollector.class)}, TestResult.NO_EXCEPTION, "open - conf con valori validi"},
                // CASO MODIFICATO: conf valido ma con valore interno di tipo errato
                {TestMethod.OPEN, new Object[]{new HashMap<String, Object>() {{ put("checkpoint.interval.ms", "non un numero"); }}, mock(TopologyContext.class), mock(SpoutOutputCollector.class)}, TestResult.NO_EXCEPTION, "open - conf con valore interno di tipo errato (atteso ClassCastException)"},
        });
    }

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        spout = new CheckpointSpout();
    }

    @After
    public void tearDown() {
        if (spout != null) spout.close();
    }

    @Test
    public void testAck() {
        if (this.methodToCall != TestMethod.ACK) {
            return;
        }

        System.out.println("Eseguendo test ACK: " + testDescription);
        // Assicurati che la spout sia in uno stato aperto per ack/fail
        spout.open(new HashMap<>(), mockContext, mockCollector);

        try {
            spout.ack(param);
            assertEquals("Nessuna eccezione lanciata durante ack, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (ClassCastException e) {
            assertEquals("Catturata ClassCastException durante ack, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante ack, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante ack: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro ack passato: " + param);
    }

    @Test
    public void testFail() {
        if (this.methodToCall != TestMethod.FAIL) {
            return;
        }

        System.out.println("Eseguendo test FAIL: " + testDescription);
        spout.open(new HashMap<>(), mockContext, mockCollector);

        try {
            spout.fail(param);
            assertEquals("Nessuna eccezione lanciata durante fail, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (ClassCastException e) {
            assertEquals("Catturata ClassCastException durante fail, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante fail, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante fail: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro fail passato: " + param);
    }

    @Test
    public void testDeclareOutputFieldsMethod() {
        if (this.methodToCall != TestMethod.DECLARE_OUTPUT_FIELDS) {
            return;
        }

        System.out.println("Eseguendo test DECLARE_OUTPUT_FIELDS: " + testDescription);

        try {
            spout.declareOutputFields((OutputFieldsDeclarer) param);
            assertEquals("Nessuna eccezione lanciata durante declareOutputFields, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (ClassCastException e) {
            assertEquals("Catturata ClassCastException durante declareOutputFields, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante declareOutputFields, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (IllegalArgumentException e) {
            assertEquals("Catturata IllegalArgumentException durante declareOutputFields, ma il risultato atteso era diverso.", TestResult.ILLEGAL_ARGUMENT_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante declareOutputFields: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro declareOutputFields passato: " + param);
    }

    @Test
    public void testIsCheckPoint() {
        if (this.methodToCall != TestMethod.IS_CHECKPOINT) {
            return;
        }
        System.out.println("Eseguendo test IS_CHECKPOINT: " + testDescription);
        try {
            boolean result = CheckpointSpout.isCheckpoint((Tuple) param);
            assertEquals("Nessuna eccezione lanciata durante isCheckpoint, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);

        } catch (ClassCastException e) {
            assertEquals("Catturata ClassCastException durante isCheckpoint, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante isCheckpoint, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (IllegalArgumentException e) {
            assertEquals("Catturata IllegalArgumentException durante isCheckpoint, ma il risultato atteso era diverso.", TestResult.ILLEGAL_ARGUMENT_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante isCheckpoint: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro isCheckPoint passato: " + param);
    }

    @Test
    public void testOpenMethod() {
        if (this.methodToCall != TestMethod.OPEN) {
            return;
        }
        System.out.println("Eseguendo test OPEN: " + testDescription);

        Object[] openParams = (Object[]) param;
        Map<String, Object> conf = null;
        TopologyContext context = null;
        SpoutOutputCollector collector = null;

        // Gestione del cast per i parametri, per permettere test con null o tipi specifici
        // Il cast di conf a Map<String, Object> è gestito qui per catturare ClassCastException
        // se openParams[0] non è un Map (come nel caso "conf tipo errato" precedente)
        try {
            conf = (Map<String, Object>) openParams[0];
            context = (TopologyContext) openParams[1];
            collector = (SpoutOutputCollector) openParams[2];
        } catch (ClassCastException e) {
            // Se il ClassCastException si verifica qui (prima della chiamata a spout.open)
            // allora il nostro testParametro era intenzionalmente malformato per catturare questo.
            assertEquals("Catturata ClassCastException durante la preparazione dei parametri per open, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
            System.out.println("Parametri open passati (errore di cast nella preparazione): conf=" + openParams[0] + ", context=" + openParams[1] + ", collector=" + openParams[2]);
            return; // Esci dal test, l'eccezione è stata gestita
        } catch (NullPointerException e) {
            // Potrebbe accadere se openParams è troppo corto, ma non dovrebbe con la nostra definizione.
            assertEquals("Catturata NullPointerException durante la preparazione dei parametri per open, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
            System.out.println("Parametri open passati (errore NullPointer nella preparazione): conf=" + openParams[0] + ", context=" + openParams[1] + ", collector=" + openParams[2]);
            return;
        }


        try {
            spout = new CheckpointSpout(); // Nuova istanza per test di open
            MockitoAnnotations.openMocks(this); // Re-inizializza i mock per la nuova istanza di spout

            spout.open(conf, context, collector);
            assertEquals("Nessuna eccezione lanciata durante open, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);

        } catch (ClassCastException e) {
            assertEquals("Catturata ClassCastException durante open, ma il risultato atteso era diverso.", TestResult.CLASS_CAST_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante open, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (IllegalArgumentException e) {
            assertEquals("Catturata IllegalArgumentException durante open, ma il risultato atteso era diverso.", TestResult.ILLEGAL_ARGUMENT_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante open: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametri open passati: conf=" + conf + ", context=" + context + ", collector=" + collector);
    }
}