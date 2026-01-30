package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


import java.io.IOException;
import java.nio.file.Files;


@RunWith(Parameterized.class)
public class UploadFilesTest {

    private final List<String> fileNames;
    private final boolean cleanupIfFails;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    private DependencyUploader uploader;
    private List<File> tempFiles;

    public UploadFilesTest(List<String> fileNames,
                           boolean cleanupIfFails,
                           Class<? extends Throwable> expectedException,
                           String description) {
        this.fileNames = fileNames;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {null, true, null, "Lista null"}, // nessuna eccezione attesa
                {null, false, null, "Lista null senza cleanup"}, // nessuna eccezione attesa
                {Collections.emptyList(), true, null, "Lista vuota"},
                {Collections.emptyList(), false, null, "Lista vuota senza cleanup"},
                {Collections.singletonList("file1.txt"), true, null, "Singolo file valido"},
                {Collections.singletonList("nonexistent.txt"), true,FileNotAvailableException.class, "File inesistente"},
        });
    }


    @Before
    public void setUp() throws Exception {
        uploader = new DependencyUploader();

        // Mock robusto del blobstore
        ClientBlobStore mockBlobStore = mock(ClientBlobStore.class);
        try {
            when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new org.apache.storm.generated.KeyNotFoundException());
            when(mockBlobStore.createBlob(anyString(), any())).thenReturn(mock(org.apache.storm.blobstore.AtomicOutputStream.class));
        } catch (Exception ignored) {}


        uploader.setBlobStore(mockBlobStore);

        // Creazione file temporanei (o simulati)
        tempFiles = new ArrayList<>();
        if (fileNames != null) {
            for (String name : fileNames) {
                File f;
                if (name.contains("nonexistent")) {
                    f = File.createTempFile("fake_", ".txt");
                    Files.deleteIfExists(f.toPath()); // simula file mancante
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
    public void testUploadFiles() {
        if (expectedException != null) {
            assertThrows(expectedException, () ->
                    uploader.uploadFiles(tempFiles, cleanupIfFails));
        } else {
            try {
                List<String> keys = uploader.uploadFiles(tempFiles, cleanupIfFails);
                assertNotNull(keys);
                if (tempFiles == null || tempFiles.isEmpty()) {
                    assertTrue(keys.isEmpty());
                } else {
                    assertEquals(tempFiles.size(), keys.size());
                    assertEquals(new HashSet<>(keys).size(), keys.size());
                }
            } catch (Throwable t) {
                fail("Eccezione inattesa: " + t);
            }
        }
    }







}
