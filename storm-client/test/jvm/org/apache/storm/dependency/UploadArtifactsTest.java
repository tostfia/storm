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
public class UploadArtifactsTest {

    private final Map<String, File> artifactsParam;
    private final Class<? extends Throwable> expectedOverallExceptionType;
    private final String testDescription;
    private final Map<String, Boolean> fileExistsStatus;

    private DependencyUploader dependencyUploader;
    private ClientBlobStore mockBlobStore;

    public UploadArtifactsTest(Map<String, File> artifactsParam,
                               Class<? extends Throwable> expectedOverallExceptionType,
                               String testDescription,
                               Map<String, Boolean> fileExistsStatus) {
        this.artifactsParam = artifactsParam;
        this.expectedOverallExceptionType = expectedOverallExceptionType;
        this.testDescription = testDescription;
        this.fileExistsStatus = fileExistsStatus != null ? fileExistsStatus : Collections.emptyMap();
    }

    @Parameterized.Parameters(name = "{index}: {2}")
    public static Collection<Object[]> getTestParameters() {
        Function<String, File> createMockFile = name -> {
            File file = mock(File.class);
            when(file.getName()).thenReturn(name);
            when(file.getAbsolutePath()).thenReturn("/tmp/" + name);
            when(file.exists()).thenReturn(true);
            return file;
        };

        // File simulati
        File file1 = createMockFile.apply("artifact1.jar");
        File file2 = createMockFile.apply("artifact2.jar");
        File file_nonExistent = createMockFile.apply("non_existent_artifact.jar");
        File file_emptyName = createMockFile.apply("");
        File fileThrowing= createMockFile.apply("throwingFile.jar");
        File file_keyExists = createMockFile.apply("key_exists_artifact.jar");

        Map<String, Boolean> allFilesExist = new HashMap<>();
        allFilesExist.put(file1.getName(), true);
        allFilesExist.put(file2.getName(), true);
        allFilesExist.put(file_emptyName.getName(), true);
        allFilesExist.put(fileThrowing.getName(), true);
        allFilesExist.put(file_keyExists.getName(), true);

        Map<String, Boolean> fileNotExistsSpecific = new HashMap<>(allFilesExist);
        fileNotExistsSpecific.put(file_nonExistent.getName(), false);

        List<Object[]> params = new ArrayList<>();

        // Input null
        params.add(new Object[]{null, NullPointerException.class,
                "uploadArtifacts - mappa di input null", new HashMap<>()});

        //Lista vuota
        params.add(new Object[]{new HashMap<>(), null,
                "uploadArtifacts - mappa vuota", new HashMap<>()});
        //Singolo file valido
        Map<String, File> singleArtifact = new HashMap<>();
        singleArtifact.put("art1", file1);
        params.add(new Object[]{singleArtifact, FileNotAvailableException.class,
                "uploadArtifacts - singolo artefatto valido", allFilesExist});
        //Più file validi
        Map<String, File> multipleArtifacts = new HashMap<>();
        multipleArtifacts.put("art1", file1);
        multipleArtifacts.put("art2", file2);
        params.add(new Object[]{multipleArtifacts, FileNotAvailableException.class,
                "uploadArtifacts - più artefatti validi", allFilesExist});
        //Chiave null
        Map<String, File> nullKey = new HashMap<>();
        nullKey.put(null, file1);
        params.add(new Object[]{nullKey, FileNotAvailableException.class,
                "uploadArtifacts - chiave null", allFilesExist});

        Map<String, File> emptyKey = new HashMap<>();
        emptyKey.put("", file1);
        params.add(new Object[]{emptyKey, FileNotAvailableException.class,
                "uploadArtifacts - chiave vuota", allFilesExist});

        Map<String, File> nullValue = new HashMap<>();
        nullValue.put("art1", null);
        params.add(new Object[]{nullValue, NullPointerException.class,
                "uploadArtifacts - file null nella mappa", allFilesExist});

        Map<String, File> nonExistentFile = new HashMap<>();
        nonExistentFile.put("nonExistArt", file_nonExistent);
        params.add(new Object[]{nonExistentFile, FileNotAvailableException.class,
                "uploadArtifacts - file non esistente", fileNotExistsSpecific});

        // File che simula eccezione interna
        Map<String, File> throwingFile = new HashMap<>();
        throwingFile.put("throw", fileThrowing);
        params.add(new Object[]{throwingFile, RuntimeException.class, "Upload fallisce (simulazione throw)", allFilesExist});

        Map<String, File> keyExists = new HashMap<>();
        keyExists.put("keyExistArt", file_keyExists);
        params.add(new Object[]{keyExists, RuntimeException.class,
                "uploadArtifacts - KeyAlreadyExistsException (incapsulata)", allFilesExist});

        // Test per il branch dove KeyAlreadyExistsException viene catturata e ignorata
        Map<String, File> keyExistsButIgnored = new HashMap<>();
        keyExistsButIgnored.put("keyExistButIgnored", file_keyExists);
        params.add(new Object[]{
                keyExistsButIgnored,
                FileNotAvailableException.class,
                "uploadArtifacts - KeyAlreadyExistsException catturata e ignorata",
                allFilesExist
        });

        return params;
    }

    @Before
    public void setup() throws AuthorizationException, KeyNotFoundException, KeyAlreadyExistsException {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();

        mockBlobStore = mock(ClientBlobStore.class);
        doNothing().when(mockBlobStore).deleteBlob(isA(String.class));
        when (mockBlobStore.getBlobMeta(isA(String.class))).thenReturn(new ReadableBlobMeta());
        dependencyUploader.setBlobStore(mockBlobStore);
    }

    @After
    public void teardown() {
        dependencyUploader.shutdown();
    }

    @Test
    public void uploadArtifactsTest() throws Exception {
        System.out.println("Eseguendo test UPLOAD_ARTIFACTS: " + testDescription);

        try {
            List<String> uploadedKeys = dependencyUploader.uploadArtifacts(artifactsParam);

            if (expectedOverallExceptionType != null) {
                fail("Prevista eccezione di tipo " + expectedOverallExceptionType.getSimpleName() +
                        " ma nessuna eccezione è stata lanciata.");
            }
            if (!artifactsParam.isEmpty()) {
                assertEquals(artifactsParam.size(),uploadedKeys.size());
                Set<String> uniqueKeys = new HashSet<>(uploadedKeys);
                assertEquals(uploadedKeys.size(), uniqueKeys.size());
                uploadedKeys.forEach(Assert::assertNotNull);
            } else {
                assertTrue(uploadedKeys.isEmpty());
            }

        } catch (Throwable t) {
            if (expectedOverallExceptionType == null) {
                fail("Eccezione non prevista: " + t.getClass().getSimpleName());
            }
            assertTrue(expectedOverallExceptionType.isInstance(t));
        }

    }
}
