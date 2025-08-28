package org.apache.storm.dependency;



import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class DeleteBlobsTestLlm {

    private DependencyUploader uploader;
    private ClientBlobStore mockStore;

    @Before
    public void setup() {
        uploader = new DependencyUploader();
        mockStore = mock(ClientBlobStore.class);
        uploader.setBlobStore(mockStore);
    }

    @Test
    public void testDeleteBlobsSuccess() throws AuthorizationException, KeyNotFoundException {
        uploader.deleteBlobs(Arrays.asList("key1", "key2"));
        verify(mockStore).deleteBlob("key1");
        verify(mockStore).deleteBlob("key2");
    }

    @Test
    public void testDeleteBlobsWithException() throws AuthorizationException, KeyNotFoundException {
        doThrow(new RuntimeException("fail")).when(mockStore).deleteBlob("key1");
        uploader.deleteBlobs(Arrays.asList("key1", "key2")); // should continue despite exception
        verify(mockStore).deleteBlob("key2");
    }
}
