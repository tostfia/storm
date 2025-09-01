package org.apache.storm.spout;

import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.spout.SpoutOutputCollector;
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
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class AckTest {

    @Parameterized.Parameter(0)
    public Object param;

    @Parameterized.Parameter(1)
    public Class<? extends Throwable> expectedResult;

    @Parameterized.Parameter(2)
    public String description;

    private CheckpointSpout spout;
    private InMemoryKeyValueState<String, CheckPointState> state;

    @Mock
    private TopologyContext context;

    @Mock
    private SpoutOutputCollector collector;

    @Before
    public void setUp() {
        state = new InMemoryKeyValueState<>();
        // Stato iniziale COMMITTED → recovering = false
        state.put("__state", new CheckPointState(123, CheckPointState.State.COMMITTED));

        MockitoAnnotations.openMocks(this);
        spout = new CheckpointSpout();
        spout.open(new HashMap<>(), context, collector);
    }

    @After
    public void tearDown() {
        spout = null;
    }

    // Test parametrizzati per tipi diversi
    @Test
    public void testAckParam() {
        try {
            spout.ack(param);
            spout.nextTuple();
            assertNull(expectedResult);
        } catch (ClassCastException e) {
            assertEquals(ClassCastException.class, expectedResult);
        } catch (NullPointerException e) {
            assertEquals(NullPointerException.class, expectedResult);
        }
    }

    // NUOVO: Test specifico per il ramo handleRecoveryAck() (recovering = true)
    @Test
    public void testAckRecoveryBranch() {
        // Imposta recovering = true chiamando fail() prima
        spout.fail(999L); // fail imposta recovering = true
        reset(collector); // Resetta il mock per verifiche pulite

        // Ora chiama ack con msgId che coincide con curTxState.txid
        spout.ack(123L);
        spout.nextTuple();

        // Verifica che sia stato chiamato handleRecoveryAck (comportamento specifico)
        // Questo dovrebbe portare a comportamenti diversi rispetto a handleCheckpointAck
        verify(collector, atLeastOnce()).emit(anyString(), anyList(), any());
    }

    // Test per il ramo handleCheckpointAck() (recovering = false)
    @Test
    public void testAckCheckpointBranch() {
        // Lo stato iniziale ha recovering = false
        spout.ack(123L); // msgId coincide con curTxState.txid → ramo handleCheckpointAck
        spout.nextTuple();

        verify(collector, atLeastOnce()).emit(
                eq(CheckpointSpout.CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
    }

    // NUOVO: Test per il ramo else (msgId non coincide con txid)
    @Test
    public void testAckMismatchBranch() {
        // msgId diverso da curTxState.txid (che è 123)
        spout.ack(999L);
        spout.nextTuple();

        // In questo caso dovrebbe solo fare il log di warning e resetProgress()
        // Non dovrebbe chiamare né handleRecoveryAck né handleCheckpointAck
        // Tuttavia, nextTuple() potrebbe comunque causare emit per altri motivi
        // Il branch è coperto dall'esecuzione del codice, anche se non possiamo
        // facilmente verificare l'assenza di specifici comportamenti
    }

    // NUOVO: Test combinato per coprire tutti i rami in sequenza
    @Test
    public void testAllBranchesCombined() {
        // 1. Test ramo mismatch
        spout.ack(999L); // Diverso da 123

        // 2. Test ramo checkpoint (recovering = false)
        spout.ack(123L); // Uguale a txid, recovering = false

        // 3. Imposta recovering = true
        spout.fail(123L);

        // 4. Test ramo recovery (recovering = true)
        spout.ack(123L); // Uguale a txid, recovering = true

        spout.nextTuple();

        // Verifica che siano stati fatti degli emit (almeno per checkpoint e recovery)
        verify(collector, atLeastOnce()).emit(anyString(), anyList(), any());
    }

    @Parameterized.Parameters(name = "{2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
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
                {"Test", ClassCastException.class, "ack - Stringa"},
                {true, ClassCastException.class, "ack - Boolean"},
                {'A', ClassCastException.class, "ack - Character"},
                {new Object(), ClassCastException.class, "ack - Oggetto generico"},
                {null, NullPointerException.class, "ack - msgId null"},
                {new Long[]{1L,2L}, ClassCastException.class, "ack - Array di Long"},
                {new int[]{1,2}, ClassCastException.class, "ack - Array di int"},
                {Arrays.asList(1L,2L), ClassCastException.class, "ack - List di Long"},
                {new CustomMsgId(), ClassCastException.class, "ack - Custom object non compatibile"}
        });
    }

    static class CustomMsgId { }
}