package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class UploadFilesTest {

    private final List<File> filesParam;
    private final boolean cleanupIfFails;
    private final Class<? extends Throwable> expectedException;
    private final String description;
    private final Map<String, Boolean> fileExistsStatus;

    private DependencyUploader dependencyUploader;
    private ClientBlobStore mockBlobStore;

    public UploadFilesTest(List<File> filesParam,
                           boolean cleanupIfFails,
                           Class<? extends Throwable> expectedException,
                           String description,
                           Map<String, Boolean> fileExistsStatus) {
        this.filesParam = filesParam;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedException = expectedException;
        this.description = description;
        this.fileExistsStatus = fileExistsStatus != null ? fileExistsStatus : Collections.emptyMap();
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> getParameters() {
        Function<String, File> createMockFile = name -> {
            File f = mock(File.class);
            when(f.getName()).thenReturn(name);
            when(f.getAbsolutePath()).thenReturn("/tmp/" + name);
            when(f.exists()).thenReturn(true);
            return f;
        };

        File file1 = createMockFile.apply("file1.txt");
        File file2 = createMockFile.apply("file2.txt");
        File fileNonExist = createMockFile.apply("nonexistent.txt");
        File fileThrowing = createMockFile.apply("throwingFile.txt");
        File fileKeyExists = createMockFile.apply("key_exists.txt");

        Map<String, Boolean> allExist = new HashMap<>();
        allExist.put(file1.getName(), true);
        allExist.put(file2.getName(), true);
        allExist.put(fileThrowing.getName(), true);
        allExist.put(fileKeyExists.getName(), true);

        Map<String, Boolean> notExistMap = new HashMap<>(allExist);
        notExistMap.put(fileNonExist.getName(), false);

        List<Object[]> params = new ArrayList<>();

        params.add(new Object[]{null, true, NullPointerException.class, "Lista null", Collections.emptyMap()});
        params.add(new Object[]{Collections.emptyList(), true, null, "Lista vuota", Collections.emptyMap()});
        params.add(new Object[]{List.of(file1), true, FileNotAvailableException.class, "Singolo file valido", allExist});
        params.add(new Object[]{List.of(file1, file2), true, FileNotAvailableException.class, "Più file validi", allExist});
        params.add(new Object[]{List.of(fileNonExist), true, FileNotAvailableException.class, "File non esistente cleanup=true", notExistMap});
        params.add(new Object[]{List.of(fileNonExist), false, FileNotAvailableException.class, "File non esistente cleanup=false", notExistMap});
        params.add(new Object[]{List.of(fileThrowing), true, RuntimeException.class, "Upload fallisce (simulazione throw)", allExist});
        params.add(new Object[]{List.of(fileKeyExists), true, RuntimeException.class, "KeyAlreadyExistsException simulata", allExist});
        params.add(new Object[]{List.of(fileKeyExists), true, RuntimeException.class,
                "KeyAlreadyExistsException catturata", allExist});

        params.add(new Object[]{List.of(fileThrowing), true, RuntimeException.class,
                "Upload fallisce con cleanup=true", allExist});

        params.add(new Object[]{List.of(fileThrowing), false, RuntimeException.class,
                "Upload fallisce con cleanup=false", allExist});


        return params;
    }

    @Before
    public void setup() throws AuthorizationException, KeyNotFoundException, KeyAlreadyExistsException {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();

        mockBlobStore = mock(ClientBlobStore.class);
        doNothing().when(mockBlobStore).deleteBlob(anyString());
        when(mockBlobStore.getBlobMeta(anyString())).thenReturn(new ReadableBlobMeta());
        when(mockBlobStore.createBlob(anyString(), any())).thenReturn(null);


        // Simula eccezioni per file particolari
        if (filesParam != null) {
            for (File f : filesParam) {
                if (f != null && "throwingFile.txt".equals(f.getName())) {
                    try {
                        doThrow(new RuntimeException("Simulated upload failure"))
                                .when(mockBlobStore).createBlob(anyString(), any());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                if (f != null && "key_exists.txt".equals(f.getName())) {
                    doThrow(new KeyAlreadyExistsException())
                            .when(mockBlobStore).createBlob(anyString(), any());
                }

            }
        }

        dependencyUploader.setBlobStore(mockBlobStore);

        if (filesParam != null) {
            for (File f : filesParam) {
                if (f != null) {
                    Boolean exists = fileExistsStatus.getOrDefault(f.getName(), true);
                    when(f.exists()).thenReturn(exists);
                }
            }
        }
    }

    @After
    public void teardown() {
        dependencyUploader.shutdown();
    }

    @Test
    public void testUploadFiles() {
        System.out.println("Test: " + description);

        try {
            List<String> keys = dependencyUploader.uploadFiles(filesParam, cleanupIfFails);

            if (expectedException != null) {
                fail("Prevista eccezione: " + expectedException.getSimpleName() + ", ma nessuna è stata lanciata");
            }

            if (filesParam != null && !filesParam.isEmpty()) {
                assertEquals(filesParam.size(), keys.size());
                Set<String> uniqueKeys = new HashSet<>(keys);
                assertEquals(keys.size(), uniqueKeys.size());
                keys.forEach(Assert::assertNotNull);
            } else {
                assertTrue(keys.isEmpty());
            }

        } catch (Throwable t) {
            if (expectedException == null) {
                fail("Eccezione non prevista: " + t.getClass().getSimpleName());
            }
            assertTrue(expectedException.isInstance(t));
        }
    }
}
