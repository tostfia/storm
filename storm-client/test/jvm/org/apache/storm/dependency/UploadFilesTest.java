package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class UploadFilesTest {

    private final List<String> fileNames;
    private final boolean cleanupIfFails;
    private final Class<? extends Throwable> expectedException;
    private final String description;
    private final boolean failOnSecondUpload;

    private DependencyUploader uploader;
    private ClientBlobStore blobStore;
    private List<File> tempFiles;

    public UploadFilesTest(List<String> fileNames,
                           boolean cleanupIfFails,
                           Class<? extends Throwable> expectedException,
                           String description,
                           boolean failOnSecondUpload) {
        this.fileNames = fileNames;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedException = expectedException;
        this.description = description;
        this.failOnSecondUpload = failOnSecondUpload;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {null, true, null, "Lista null", false},
                {Collections.emptyList(), true, null, "Lista vuota", false},
                {Collections.singletonList("file1.txt"), true, null, "Singolo file valido", false},
                {Collections.singletonList("missing.txt"), true, FileNotAvailableException.class, "File inesistente", false},
                {Arrays.asList("file1.txt", "file2.txt"), true, RuntimeException.class,
                        "Fallimento su secondo upload con cleanup", true}
        });
    }

    @Before
    public void setUp() throws Exception {
        uploader = new DependencyUploader();

        blobStore = mock(ClientBlobStore.class);
        uploader.setBlobStore(blobStore);

        when(blobStore.getBlobMeta(anyString()))
                .thenThrow(new KeyNotFoundException());

        AtomicOutputStream out = mock(AtomicOutputStream.class);

        if (failOnSecondUpload) {
            when(blobStore.createBlob(anyString(), any()))
                    .thenReturn(out)
                    .thenThrow(new RuntimeException("Upload failure"));
        } else {
            when(blobStore.createBlob(anyString(), any()))
                    .thenReturn(out);
        }

        tempFiles = new ArrayList<>();
        if (fileNames != null) {
            for (String name : fileNames) {
                File f;
                if (name.contains("missing")) {
                    f = File.createTempFile("missing", ".txt");
                    Files.deleteIfExists(f.toPath());
                } else {
                    f = File.createTempFile(name.replace(".txt", ""), ".txt");
                    Files.write(f.toPath(), "data".getBytes());
                    f.deleteOnExit();
                }
                tempFiles.add(f);
            }
        }
    }

    @After
    public void tearDown() {
        uploader.shutdown();
        if (tempFiles != null) {
            for (File f : tempFiles) {
                try {
                    Files.deleteIfExists(f.toPath());
                } catch (IOException ignored) {}
            }
        }
    }

    @Test
    public void testUploadFiles() throws AuthorizationException, KeyNotFoundException, IOException {
        if (expectedException != null) {
            assertThrows(expectedException,
                    () -> uploader.uploadFiles(tempFiles, cleanupIfFails));

            if (failOnSecondUpload && cleanupIfFails) {
                verify(blobStore, atLeastOnce()).deleteBlob(anyString());
            }
        } else {
            List<String> keys = uploader.uploadFiles(tempFiles, cleanupIfFails);
            assertNotNull(keys);

            if (tempFiles == null || tempFiles.isEmpty()) {
                assertTrue(keys.isEmpty());
            } else {
                assertEquals(tempFiles.size(), keys.size());
            }
        }
    }
}
