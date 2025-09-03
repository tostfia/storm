package org.apache.storm.dependency;

import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class UploadArtifactsTest {

    private final List<String> artifactNames;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    private DependencyUploader uploader;
    private ClientBlobStore mockBlobStore;
    private Map<String, File> tempArtifacts;

    public UploadArtifactsTest(List<String> artifactNames,
                               Class<? extends Throwable> expectedException,
                               String description) {
        this.artifactNames = artifactNames;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {null, NullPointerException.class, "Mappa null"},
                {Collections.emptyList(), null, "Lista vuota"},
                {Collections.singletonList("artifact1.jar"), null, "Singolo file valido"},
                {Arrays.asList("artifact1.jar", "artifact2.jar"), null, "Più file validi"},
                {Collections.singletonList("nonexistent.jar"), RuntimeException.class, "File inesistente"},
                {Arrays.asList("artifact1.jar", "nonexistent.jar"), RuntimeException.class, "Mix file valido e inesistente"},
                {Collections.singletonList("existing-key.jar"), null, "Artifact con chiave già esistente"},
                {Arrays.asList("artifact1.jar", "existing-key.jar"), null, "Mix artifact normale e chiave esistente"}

        });
    }

    @Before
    public void setUp() throws AuthorizationException, KeyNotFoundException, KeyAlreadyExistsException, IOException {
        uploader = new DependencyUploader();

        // Mock BlobStore
        mockBlobStore = mock(ClientBlobStore.class);
        doNothing().when(mockBlobStore).deleteBlob(anyString());
        when(mockBlobStore.getBlobMeta(anyString())).thenReturn(new ReadableBlobMeta());
        uploader.setBlobStore(mockBlobStore);

        // Creazione file temporanei
        tempArtifacts = new HashMap<>();
        if (artifactNames != null) {
            for (String name : artifactNames) {
                if (!name.contains("nonexistent")) {
                    File temp = File.createTempFile(name.replace(".jar", ""), ".jar");
                    temp.deleteOnExit();
                    tempArtifacts.put(name, temp);
                } else {
                    tempArtifacts.put(name, new File("/tmp/nonexistent.jar"));
                    // Mock per simulare KeyAlreadyExistsException per artifact "existing-key"
                    if (name.contains("existing-key")) {
                        doThrow(new KeyAlreadyExistsException("Key already exists"))
                                .when(mockBlobStore).createBlob(anyString(), any());
                    }

                }
            }
        } else {
            tempArtifacts = null;
        }
    }

    @After
    public void tearDown() {
        uploader.shutdown();
        if (tempArtifacts != null) {
            for (File f : tempArtifacts.values()) {
                if (f.exists()) f.delete();
            }
        }
    }

    @Test
    public void testUploadArtifacts() {
        System.out.println("Test: " + description);

        try {
            List<String> keys = uploader.uploadArtifacts(tempArtifacts);

            // Verifica che le chiavi siano corrette
            if (tempArtifacts != null && !tempArtifacts.isEmpty()) {
                assertEquals(tempArtifacts.size(), keys.size());
                assertEquals(new HashSet<>(keys).size(), keys.size()); // chiavi uniche
                keys.forEach(Assert::assertNotNull);
            } else {
                assertTrue(keys.isEmpty());
            }

            if (expectedException != null) {
                fail("Mi aspettavo un'eccezione di tipo " + expectedException.getSimpleName());
            }

        } catch (Throwable t) {
            if (expectedException != null) {
                assertTrue("Tipo eccezione attesa: " + expectedException.getSimpleName() +
                                ", trovata: " + t.getClass().getSimpleName(),
                        expectedException.isInstance(t));
            } else {
                fail("Eccezione non attesa: " + t);
            }
        }
    }
}
