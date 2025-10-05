package org.apache.storm.dependency;

import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.ClientBlobStore;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class UploadFilesTest {

    private final List<String> fileNames;
    private final boolean cleanupIfFails;
    private final Class<? extends Throwable> expectedException;

    private DependencyUploader uploader;

    public UploadFilesTest(List<String> fileNames,
                           boolean cleanupIfFails,
                           Class<? extends Throwable> expectedException,
                           String description) {
        this.fileNames = fileNames;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Collections.emptyList(), true, null, "Lista vuota"},
                {Collections.singletonList("valid-file.jar"), true, null, "Singolo file valido"},
                {Arrays.asList("file1.jar", "file2.jar"), true, null, "Più file validi"},
                {Collections.singletonList("nonexistent.jar"), true, RuntimeException.class, "File inesistente cleanup=true"},
                {Collections.singletonList("nonexistent.jar"), false, RuntimeException.class, "File inesistente cleanup=false"}
        });
    }

    @Before
    public void setUp() throws Exception {
        uploader = new DependencyUploader();

        // Mock BlobStore
        ClientBlobStore mockBlobStore = mock(ClientBlobStore.class);
        try {
            when(mockBlobStore.getBlobMeta(anyString()))
                    .thenThrow(new org.apache.storm.generated.KeyNotFoundException());

            AtomicOutputStream mockStream = mock(AtomicOutputStream.class);
            doNothing().when(mockStream).close();

            when(mockBlobStore.createBlob(anyString(), any())).thenReturn(mockStream);

        } catch (Exception ignored) {}

        uploader.setBlobStore(mockBlobStore);
    }

    @After
    public void tearDown() {
        uploader.shutdown();
    }

    @Test
    public void testUploadFiles() {
        List<File> realFiles = new ArrayList<>();

        // Creiamo file temporanei solo se il nome non contiene "nonexistent"
        for (String name : fileNames) {
            if (!name.contains("nonexistent")) {
                try {
                    File temp = File.createTempFile(name.replace(".jar", ""), ".jar");
                    temp.deleteOnExit();
                    realFiles.add(temp);
                } catch ( IOException e) {
                    fail("Errore creazione file temporaneo: " + e);
                }
            } else {
                // File inesistente
                realFiles.add(new File("/tmp/" + name));
            }
        }

        try {
            List<String> keys = uploader.uploadFiles(realFiles, cleanupIfFails);

            if (expectedException != null) {
                fail("Expected exception: " + expectedException.getSimpleName());
            }

            // Verifica chiavi generate
            if (!realFiles.isEmpty()) {
                assertNotNull(keys);
                assertEquals(realFiles.size(), keys.size());
                keys.forEach(Objects::requireNonNull);
            } else {
                assertTrue(keys.isEmpty());
            }

        } catch (Throwable t) {
            if (expectedException != null) {
                assertTrue("Expected: " + expectedException + ", but got: " + t.getClass(),
                        expectedException.isInstance(t));
            } else {
                fail("Unexpected exception: " + t);
            }
        }
    }


    //Aggiunta per pittest





}
