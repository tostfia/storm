package org.apache.storm.spout;


import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


public class NextTupleTest {
    private CheckpointSpout spout;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    private KeyValueState<String, org.apache.storm.spout.CheckPointState> state;

    // Campi per la reflection sui flag di stato
    private Field recoveringField;
    private Field recoveryStepInProgressField;
    private Field checkpointStepInProgressField;
    private Field lastCheckpointTsField;

    private static final int EXPECTED_SLEEP_INTERVAL = 10;
    private static final String ACTUAL_CHECKPOINT_STREAM_ID = CheckpointSpout.CHECKPOINT_STREAM_ID;


    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        collector = mock(SpoutOutputCollector.class);
        context = mock(TopologyContext.class);

        state = new InMemoryKeyValueState<>();
        state.put("__state", new org.apache.storm.spout.CheckPointState(-1, org.apache.storm.spout.CheckPointState.State.COMMITTED));// Stato iniziale

        spout = new CheckpointSpout();
        spout.open(context, collector, 100, state); // open imposta recovering = true

        // Inizializza i campi di reflection
        recoveringField = CheckpointSpout.class.getDeclaredField("recovering");
        recoveringField.setAccessible(true);
        recoveryStepInProgressField = CheckpointSpout.class.getDeclaredField("recoveryStepInProgress");
        recoveryStepInProgressField.setAccessible(true);
        checkpointStepInProgressField = CheckpointSpout.class.getDeclaredField("checkpointStepInProgress");
        checkpointStepInProgressField.setAccessible(true);

        lastCheckpointTsField = CheckpointSpout.class.getDeclaredField("lastCheckpointTs");
        lastCheckpointTsField.setAccessible(true);

        // --- Reset esplicito dei flag di progresso dopo open() ---
        recoveryStepInProgressField.set(spout, false);
        checkpointStepInProgressField.set(spout, false);

        // Ripuliamo eventuali emit fatti durante open()
        clearInvocations(collector);
    }

    // Metodi helper per accedere ai flag tramite reflection
    private boolean isRecovering() {
        try {
            return (boolean) recoveringField.get(spout);
        } catch (IllegalAccessException e) {
            fail("Errore nell'accesso a recovering tramite reflection: " + e.getMessage());
            return false;
        }
    }

    private boolean isRecoveryStepInProgress() {
        try {
            return (boolean) recoveryStepInProgressField.get(spout);
        } catch (IllegalAccessException e) {
            fail("Errore nell'accesso a recoveryStepInProgress tramite reflection: " + e.getMessage());
            return false;
        }
    }

    private boolean isCheckpointStepInProgress() {
        try {
            return (boolean) checkpointStepInProgressField.get(spout);
        } catch (IllegalAccessException e) {
            fail("Errore nell'accesso a checkpointStepInProgress tramite reflection: " + e.getMessage());
            return false;
        }
    }

    // Test per il ramo recovery (dovrebbe chiamare startProgress() con recovering=true)
    @Test
    public void testRecovery() throws IllegalAccessException {
        // All'inizio di questo test, `recovering` è già `true` a causa di `spout.open()` in `setup()`.
        assertTrue("Lo spout dovrebbe essere in stato 'recovering' dopo open()", isRecovering());
        assertFalse("recoveryStepInProgress dovrebbe essere false prima di nextTuple()", isRecoveryStepInProgress());
        assertFalse("checkpointStepInProgress dovrebbe essere false prima di nextTuple()", isCheckpointStepInProgress());


        // Imposta uno stato PREPARING per forzare la recovery
        state.put("__state", new org.apache.storm.spout.CheckPointState(1, org.apache.storm.spout.CheckPointState.State.PREPARING));

        spout.nextTuple(); // Esegue handleRecovery() e startProgress()

        // Verifica che un messaggio di recovery sia stato emesso
        verify(collector, atLeastOnce()).emit(
                eq(ACTUAL_CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );

        // --- Asserzione per uccidere il mutante `startProgress` nel ramo recovery ---
        // Se `recovering` è true (come dovrebbe essere), `startProgress()` imposta `recoveryStepInProgress = true`.
        assertTrue("recoveryStepInProgress dovrebbe essere true dopo la chiamata a startProgress() nel ramo recovery", isRecoveryStepInProgress());
        assertFalse("checkpointStepInProgress dovrebbe essere false nel ramo recovery", isCheckpointStepInProgress());
    }

    // Test per il ramo checkpoint (dovrebbe chiamare startProgress() con recovering=false)
    @Test
    public void testCheckpoint() throws IllegalAccessException {
        // --- AGGIUNTA QUI: Reset esplicito dei flag di progresso all'inizio del test ---
        recoveryStepInProgressField.set(spout, false);
        checkpointStepInProgressField.set(spout, false);

        recoveringField.set(spout, false); // Forziamo `recovering` a `false`

        assertFalse("Lo spout dovrebbe essere uscito dallo stato 'recovering'", isRecovering());
        assertFalse("recoveryStepInProgress dovrebbe essere false", isRecoveryStepInProgress());
        assertFalse("checkpointStepInProgress dovrebbe essere false prima del checkpoint", isCheckpointStepInProgress());


        // Imposta uno stato COMMITTING per forzare un checkpoint
        state.put("__state", new org.apache.storm.spout.CheckPointState(2, org.apache.storm.spout.CheckPointState.State.COMMITTING));

        spout.nextTuple(); // Esegue doCheckpoint() e startProgress()

        // Verifica che un messaggio di checkpoint sia stato emesso
        verify(collector, atLeastOnce()).emit(
                eq(ACTUAL_CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );

        // --- Asserzione per uccidere il mutante `startProgress` nel ramo checkpoint ---
        // Se `recovering` è false, `startProgress()` imposta `checkpointStepInProgress = true`.
        assertTrue("checkpointStepInProgress dovrebbe essere true dopo la chiamata a startProgress() nel ramo checkpoint", isCheckpointStepInProgress());
        assertFalse("recoveryStepInProgress dovrebbe essere false nel ramo checkpoint", isRecoveryStepInProgress());
    }

    // Test per uccidere il mutante `removed call to org/apache/storm/utils/Utils::sleep`
    @Test
    public void testSleepVerifiesCall() throws IllegalAccessException {
        try (MockedStatic<Utils> mockedUtils = Mockito.mockStatic(Utils.class)) {
            // Resettiamo tutti i flag di stato per iniziare da uno stato pulito per questo test
            recoveringField.set(spout, false);
            recoveryStepInProgressField.set(spout, false);
            checkpointStepInProgressField.set(spout, false);

            // 1. Impostiamo lo stato PERSISTENTE della spout a COMMITTED
            state.put("__state", new org.apache.storm.spout.CheckPointState(3, org.apache.storm.spout.CheckPointState.State.COMMITTED));

            // 2. ASSICURIAMO CHE NON SIA ANCORA ORA DI FARE UN CHECKPOINT
            //    Impostiamo `lastCheckpointTs` a un valore molto recente (es. ora).
            //    Questo dovrebbe far sì che `shouldCheckpoint()` restituisca `false`.
            //    Abbiamo bisogno del Field per lastCheckpointTs. Aggiungiamolo al setup.
            Field lastCheckpointTsField = CheckpointSpout.class.getDeclaredField("lastCheckpointTs"); // Assicurati sia accessibile
            lastCheckpointTsField.setAccessible(true);
            lastCheckpointTsField.set(spout, System.currentTimeMillis()); // Imposta il timestamp all'ora corrente.

            // Verifichiamo lo stato interno subito prima di chiamare nextTuple
            assertFalse("Lo spout non dovrebbe essere in recovering prima di sleep", isRecovering());
            assertFalse("recoveryStepInProgress dovrebbe essere false prima di sleep", isRecoveryStepInProgress());
            assertFalse("checkpointStepInProgress dovrebbe essere false prima di sleep", isCheckpointStepInProgress());


            spout.nextTuple(); // QUESTA CHIAMATA DEVE ORA ENTRARE NEL RAMO 'ELSE'

            mockedUtils.verify(() -> Utils.sleep(EXPECTED_SLEEP_INTERVAL), times(1));

            verify(collector, never()).emit(
                    eq(ACTUAL_CHECKPOINT_STREAM_ID),
                    anyList(),
                    any()
            );
            assertFalse("recoveryStepInProgress dovrebbe essere false nel ramo sleep", isRecoveryStepInProgress());
            assertFalse("checkpointStepInProgress dovrebbe essere false nel ramo sleep", isCheckpointStepInProgress());

        } catch (Exception e) {
            fail("Errore durante il mocking statico di Utils.sleep: " + e.getMessage());
        }
    }



    @Test
    public void testNextTupleCheckpointBranch() throws IllegalAccessException {
        // Simula la fine della recovery per entrare nel ramo checkpoint
        state.put("__state", new org.apache.storm.spout.CheckPointState(1, org.apache.storm.spout.CheckPointState.State.PREPARING));
        spout.nextTuple(); // Esegue recovery, imposta `recovering = false` (se handleRecovery lo fa)
        clearInvocations(collector);

        // Forza `recovering = false` per essere sicuri, dato che `spout.open` potrebbe averlo resettato a `true`
        recoveringField.set(spout, false);

        state.put("__state", new org.apache.storm.spout.CheckPointState(123, org.apache.storm.spout.CheckPointState.State.COMMITTING));

        // Questo test è un po' problematico se `spout.open` resetta `recovering = true`
        // e lo fa dopo le impostazioni precedenti.
        // Se non è strettamente necessario, potresti voler evitare di chiamare `spout.open` a metà di un test.
        // Se lo tieni, devi forzare `recovering = false` di nuovo.
        spout.open(new HashMap<>(), context, collector);
        recoveringField.set(spout, false); // <--- FORZA `recovering = false` DOPO LA SECONDA OPEN

        spout.nextTuple();

        verify(collector, atLeastOnce()).emit(
                eq(ACTUAL_CHECKPOINT_STREAM_ID),
                anyList(),
                any()
        );
        assertTrue("checkpointStepInProgress dovrebbe essere true dopo startProgress() in questo ramo", isCheckpointStepInProgress());
    }

}