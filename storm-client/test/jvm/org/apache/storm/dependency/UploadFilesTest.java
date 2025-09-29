package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
                {null, true, RuntimeException.class, "Lista null"},
                {Collections.emptyList(), true, null, "Lista vuota"},
                {Collections.singletonList("file1.txt"), true, null, "Singolo file valido"},
                {Arrays.asList("file1.txt", "file2.txt"), true, null, "Più file validi"},
                {Collections.singletonList("nonexistent.txt"), true, RuntimeException.class, "File inesistente cleanup=true"},
                {Collections.singletonList("nonexistent.txt"), false, RuntimeException.class, "File inesistente cleanup=false"},
                {Collections.singletonList("existing-key.txt"), true, null, "File con chiave già esistente cleanup=true"},
                {Collections.singletonList("existing-key.txt"), false, null, "File con chiave già esistente cleanup=false"},
                {Arrays.asList("file1.txt", "existing-key.txt"), true, null, "Mix file normale e chiave esistente"},



        });
    }

    @Before
    public void setUp() throws Exception {
        uploader = new DependencyUploader();


        ClientBlobStore mockBlobStore = mock(ClientBlobStore.class);

        when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new org.apache.storm.generated.KeyNotFoundException());
        when(mockBlobStore.createBlob(anyString(), any())).thenReturn(mock(org.apache.storm.blobstore.AtomicOutputStream.class));
        uploader.setBlobStore(mockBlobStore);

        tempFiles = new ArrayList<>();

        if (fileNames != null) {
            for (String name : fileNames) {
                File temp;

                if (!name.contains("nonexistent")) {
                    // File temporaneo reale
                    temp = File.createTempFile(name.replace(".txt", ""), ".txt");
                    temp.deleteOnExit();
                } else {
                    // File inesistente
                    temp = new File("/tmp/" + name);
                }

                tempFiles.add(temp);

            }
        } else {
            tempFiles = null;
        }

    }





    @After
    public void tearDown() {
        uploader.shutdown();
        if (tempFiles != null) {
            for (File f : tempFiles) {
                if (f.exists()) f.delete();
            }
        }
    }

    @Test
    public void testUploadFiles() {
        try {
            List<String> keys = uploader.uploadFiles(tempFiles, cleanupIfFails);

            if (tempFiles != null && !tempFiles.isEmpty()) {
                assertEquals(tempFiles.size(), keys.size());
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
                // Verifica che deleteBlobs venga chiamato se cleanupIfFails è true
                if (cleanupIfFails && tempFiles != null) {
                    verify(uploader, atLeastOnce()).deleteBlobs(anyList());
                }

            } else {
                fail("Eccezione non attesa: " + t);
            }
        }
    }



}
