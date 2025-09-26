package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class DependencyUploaderInitShutdownTest {

    private DependencyUploader uploader;
    private ClientBlobStore mockBlobStore;

    @Before
    public void setUp() {
        uploader = new DependencyUploader();
        mockBlobStore = mock(ClientBlobStore.class);
        uploader.setBlobStore(mockBlobStore); // iniettiamo il mock
    }

    @Test
    public void testInitAllowsUpload() throws Exception {
        uploader.init();
        // Se init funziona, uploadFiles() dovrebbe eseguire senza eccezioni
        uploader.uploadFiles(Collections.<File>emptyList(),true);
    }



    @Test
    public void testDoubleInitIsSafe() {
        uploader.init();
        uploader.init(); // non deve sollevare errori
    }

    @Test
    public void testShutdownWithoutInitDoesNotThrow() {
        // rimuoviamo il blobStore per simulare "senza init"
        uploader.setBlobStore(null);
        uploader.shutdown(); // dovrebbe completarsi senza eccezioni
    }

    @Test
    public void testDoubleShutdownIsSafe() {
        uploader.shutdown();
        uploader.shutdown(); // seconda chiamata sicura
        verify(mockBlobStore, times(2)).shutdown(); // se vuoi riflettere la realtà

    }


}
