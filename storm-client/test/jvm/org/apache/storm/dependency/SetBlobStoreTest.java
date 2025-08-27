package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class SetBlobStoreTest {

    private final ClientBlobStore paramBlobStore;
    private final Class<? extends Throwable> expectedOverallExceptionType;
    private final String testDescription;

    private DependencyUploader dependencyUploader;

    public SetBlobStoreTest(ClientBlobStore paramBlobStore, Class<? extends Throwable> expectedOverallExceptionType, String testDescription) {
        this.paramBlobStore = paramBlobStore;
        this.expectedOverallExceptionType = expectedOverallExceptionType;
        this.testDescription = testDescription;
    }

    @Parameterized.Parameters(name = "{index}: {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Partizione: Input valido (ClientBlobStore non nullo)
                {mock(ClientBlobStore.class), null, "setBlobStore - ClientBlobStore valido"},
                // Partizione: Input nullo
                {null, null, "setBlobStore - ClientBlobStore nullo"}, // Setter dovrebbe accettare null senza eccezioni
        });
    }

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();
    }

    @After
    public void teardown() {
        dependencyUploader.shutdown();
    }

    @Test
    public void setBlobStoreTest() {
        System.out.println("Eseguendo test SET_BLOB_STORE: " + testDescription);

        try {
            dependencyUploader.setBlobStore(paramBlobStore);
            if (expectedOverallExceptionType != null) {
                fail("Prevista eccezione di tipo " + expectedOverallExceptionType.getSimpleName() + " ma nessuna eccezione è stata lanciata.");
            }


        } catch (Throwable caughtException) {
            if (expectedOverallExceptionType == null) {
                fail("Nessuna eccezione prevista, ma è stata catturata: " + caughtException.getClass().getSimpleName() + " con messaggio: " + caughtException.getMessage());
            }
            assertEquals("Tipo di eccezione atteso " + expectedOverallExceptionType.getSimpleName() +
                            " ma catturato " + caughtException.getClass().getSimpleName(),
                    expectedOverallExceptionType, caughtException.getClass());
        }
        System.out.println("Parametro setBlobStore passato: " + (paramBlobStore == null ? "null" : paramBlobStore.getClass().getSimpleName()));
    }
}