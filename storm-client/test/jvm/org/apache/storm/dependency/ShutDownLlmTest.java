package org.apache.storm.dependency;



import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class ShutDownLlmTest {

    @Test
    public void testShutdownWithBlobStore() {
        DependencyUploader uploader = new DependencyUploader();
        ClientBlobStore mockStore = mock(ClientBlobStore.class);
        uploader.setBlobStore(mockStore);
        uploader.shutdown();
        verify(mockStore).shutdown();
    }

    @Test
    public void testShutdownWithoutBlobStore() {
        DependencyUploader uploader = new DependencyUploader();
        uploader.shutdown(); // should not throw
    }
}