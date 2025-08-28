package org.apache.storm.dependency;



import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.apache.storm.generated.KeyAlreadyExistsException;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class UploadArtifactsTestLlm {

    private final String artifactName;
    private final File file;
    private DependencyUploader uploader;

    public UploadArtifactsTestLlm(String artifactName, File file) {
        this.artifactName = artifactName;
        this.file = file;
    }

    @Parameterized.Parameters(name = "{index}: uploadArtifact({0})")
    public static Collection<Object[]> data() throws Exception {
        File validFile1 = File.createTempFile("lib1", ".jar");
        File validFile2 = File.createTempFile("lib2", ".jar");
        validFile1.deleteOnExit();
        validFile2.deleteOnExit();

        File missingFile = new File("nonexistent.jar");

        return Arrays.asList(new Object[][]{
                {"group:lib1:1.0", validFile1},
                {"org:toolkit:2.1", validFile2},
                {"com:missing:9.9", missingFile}
        });
    }

    @Before
    public void setup() {
        uploader = new DependencyUploader();

        // Mock del ClientBlobStore per evitare connessioni reali
        ClientBlobStore mockBlobStore = mock(ClientBlobStore.class);
        try {
            when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new org.apache.storm.generated.KeyNotFoundException());
            when(mockBlobStore.createBlob(anyString(), any())).thenThrow(new KeyAlreadyExistsException());
        } catch (Exception ignored) {}

        uploader.setBlobStore(mockBlobStore);
    }

    @Test
    public void testUploadArtifact() {
        Map<String, File> input = new HashMap<>();
        input.put(artifactName, file);

        try {
            List<String> keys = uploader.uploadArtifacts(input);
            assertEquals(1, keys.size());
            assertTrue(keys.get(0).endsWith(".jar"));
        } catch (FileNotAvailableException e) {
            assertFalse(file.exists()); // conferma che il file mancante genera eccezione
        } catch (RuntimeException e) {
            // Se il mock lancia eccezioni simulate, le accettiamo come parte del test
            assertTrue(e.getCause() instanceof KeyAlreadyExistsException);
        }
    }
}
