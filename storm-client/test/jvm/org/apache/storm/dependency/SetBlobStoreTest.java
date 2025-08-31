package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class SetBlobStoreTest {

    @Parameterized.Parameter(0)
    public ClientBlobStore paramBlobStore;

    @Parameterized.Parameter(1)
    public String testDescription;

    @Mock
    private ClientBlobStore mockBlobStore;

    private DependencyUploader dependencyUploader;

    @Parameterized.Parameters(name = "{index}: {1}")
    public static Object[][] data() {
        return new Object[][]{
                {mock(ClientBlobStore.class), "ClientBlobStore valido"},
                {null, "ClientBlobStore nullo"}
        };
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
    public void setBlobStoreTest() throws Exception {
        System.out.println("Eseguendo test SET_BLOB_STORE: " + testDescription);

        // Chiamata al setter
        dependencyUploader.setBlobStore(paramBlobStore);

        // Verifica indiretta: chiama deleteBlobs e controlla comportamento
        List<String> keys = Collections.singletonList("key1");

        try {
            dependencyUploader.deleteBlobs(keys);
        } catch (Throwable e) {
            fail("deleteBlobs non dovrebbe propagare eccezioni, anche se blobStore è nullo");
        }

        // Se blobStore è valido, verifica che deleteBlob sia stato chiamato
        if (paramBlobStore != null) {
            verify(paramBlobStore).deleteBlob("key1");
        }

        System.out.println("Parametro setBlobStore passato: " +
                (paramBlobStore == null ? "null" : paramBlobStore.getClass().getSimpleName()));
    }
}
