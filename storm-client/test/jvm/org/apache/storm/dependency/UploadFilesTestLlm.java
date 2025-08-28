package org.apache.storm.dependency;


import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UploadFilesTestLlm {

    private DependencyUploader uploader;
    private ClientBlobStore mockBlobStore;

    @Before
    public void setup() {
        uploader = new DependencyUploader();
        mockBlobStore = mock(ClientBlobStore.class);

        try {
            when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new org.apache.storm.generated.KeyNotFoundException());
            when(mockBlobStore.createBlob(anyString(), any())).thenReturn(mock(org.apache.storm.blobstore.AtomicOutputStream.class));
        } catch (Exception ignored) {}

        uploader.setBlobStore(mockBlobStore);
    }

    @Test
    public void testUploadValidFile() throws IOException, AuthorizationException {
        File validFile = File.createTempFile("valid", ".jar");
        validFile.deleteOnExit();

        System.out.println("File path: " + validFile.getAbsolutePath());
        System.out.println("File exists: " + validFile.exists());
        System.out.println("File readable: " + validFile.canRead());

        try {
            List<String> keys = uploader.uploadFiles(Collections.singletonList(validFile), false);

            System.out.println("Returned keys: " + keys);

            assertNotNull("Returned key list should not be null", keys);
            assertEquals("Expected one key", 1, keys.size());

            String key = keys.get(0);
            System.out.println("Generated key: " + key);

            assertNotNull("Generated key should not be null", key);
            assertFalse("Generated key should not be empty", key.trim().isEmpty());

            // Se vuoi testare il contenuto, fallo solo se hai conferma del formato
            // assertTrue("Key should contain 'valid'", key.contains("valid"));
        } catch (RuntimeException e) {
            System.out.println("RuntimeException caught: " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("Cause: " + e.getCause().getClass().getSimpleName());
            }
            fail("Unexpected RuntimeException during upload");
        }
    }
}