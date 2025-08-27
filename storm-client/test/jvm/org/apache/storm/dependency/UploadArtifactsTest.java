package org.apache.storm.dependency;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class UploadArtifactsTest {

    private final Map<String, File> artifactsParam; // Contiene Map<String, File>
    private final Class<? extends Throwable> expectedOverallExceptionType; // Usiamo Class per le eccezioni
    private final String testDescription;
    private final Map<String, Boolean> fileExistsStatus; // Per simulare File.exists()



    private DependencyUploader dependencyUploader;

    // Costruttore parametrizzato
    public UploadArtifactsTest(Map<String, File> artifactsParam, Class<? extends Throwable> expectedOverallExceptionType, String testDescription,
                               Map<String, Boolean> fileExistsStatus) {
        this.artifactsParam = artifactsParam;
        this.expectedOverallExceptionType = expectedOverallExceptionType;
        this.testDescription = testDescription;
        this.fileExistsStatus = fileExistsStatus != null ? fileExistsStatus : Collections.emptyMap();
    }

    @Parameterized.Parameters(name = "{index}: {2}")
    public static Collection<Object[]> getTestParameters() {
        // Funzione per creare un File mock
        Function<String, File> createMockFile = (name) -> {
            File mockFile = mock(File.class);
            when(mockFile.getName()).thenReturn(name);
            when(mockFile.getAbsolutePath()).thenReturn("/tmp/" + name);
            when(mockFile.exists()).thenReturn(true); // default: esiste
            return mockFile;
        };

        // File simulati
        File file1 = createMockFile.apply("artifact1.jar");
        File file2 = createMockFile.apply("artifact2.jar");
        File file_nonExistent = createMockFile.apply("non_existent_artifact.jar");
        File file_emptyName = createMockFile.apply("");
        File file_uploadFail_IO = createMockFile.apply("fail_upload_artifact_io.jar");
        File file_uploadFail_Auth = createMockFile.apply("fail_upload_artifact_auth.jar");
        File file_keyExists = createMockFile.apply("key_exists_artifact.jar");


        // Tutti i file esistono
        Map<String, Boolean> allFilesExist = new HashMap<>();
        allFilesExist.put("art1", true);
        allFilesExist.put("art2", true);
        allFilesExist.put(file1.getName(), true);
        allFilesExist.put(file2.getName(), true);
        allFilesExist.put(file_emptyName.getName(), true); // chiave vuota
        allFilesExist.put(file_uploadFail_IO.getName(), true);
        allFilesExist.put(file_uploadFail_Auth.getName(), true);
        allFilesExist.put(file_keyExists.getName(), true);


        // Variante con file inesistente
        Map<String, Boolean> fileNotExistsSpecific = new HashMap<>(allFilesExist);
        fileNotExistsSpecific.put(file_nonExistent.getName(), false);

        // Lista dei parametri
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[]{null, NullPointerException.class,
                "uploadArtifacts - mappa di input null", new HashMap<>()});

        params.add(new Object[]{new HashMap<String, File>(), null,
                "uploadArtifacts - mappa vuota", new HashMap<>()});

        Map<String, File> singleArtifact = new HashMap<>();
        singleArtifact.put("art1", file1);
        params.add(new Object[]{singleArtifact,FileNotAvailableException.class,
                "uploadArtifacts - singolo artefatto valido", allFilesExist});

        Map<String, File> multipleArtifacts = new HashMap<>();
        multipleArtifacts.put("art1", file1);
        multipleArtifacts.put("art2", file2);
        params.add(new Object[]{multipleArtifacts,FileNotAvailableException.class,
                "uploadArtifacts - più artefatti validi", allFilesExist});

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
        params.add(new Object[]{nullValue,NullPointerException.class,
                "uploadArtifacts - file null nella mappa", allFilesExist});

        Map<String, File> nonExistentFile = new HashMap<>();
        nonExistentFile.put("nonExistArt", file_nonExistent);
        params.add(new Object[]{nonExistentFile, FileNotAvailableException.class,
                "uploadArtifacts - file non esistente", fileNotExistsSpecific});

        Map<String, File> failIO = new HashMap<>();
        failIO.put("failArtIO", file_uploadFail_IO);
        params.add(new Object[]{failIO, FileNotAvailableException.class,
                "uploadArtifacts - upload fallisce (IO)", allFilesExist});

        Map<String, File> failAuth = new HashMap<>();
        failAuth.put("failArtAuth", file_uploadFail_Auth);
        params.add(new Object[]{failAuth, FileNotAvailableException.class,
                "uploadArtifacts - upload fallisce (Auth)", allFilesExist});

        Map<String, File> keyExists = new HashMap<>();
        keyExists.put("keyExistArt", file_keyExists);
        params.add(new Object[]{keyExists, RuntimeException.class,
                "uploadArtifacts - KeyAlreadyExistsException (incapsulata)", allFilesExist});


        return params;
    }



    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();



        // Configura il comportamento di File.exists() per i mock File
        if (artifactsParam != null) {
            for (Map.Entry<String, File> entry : artifactsParam.entrySet()) {
                File file = entry.getValue();
                if (file != null) {
                    Boolean exists = fileExistsStatus.get(entry.getKey()); // Cerca per chiave mappa
                    if (exists == null) {
                        exists = fileExistsStatus.get(file.getName()); // Fallback per nome file
                    }
                    when(file.exists()).thenReturn(exists != null ? exists : true); // Default: true
                }
            }
        }


    }

    @After
    public void teardown() {
        dependencyUploader.shutdown();
    }

    @Test
    public void uploadArtifactsTest() throws Exception {
        System.out.println("Eseguendo test UPLOAD_ARTIFACTS: " + testDescription);

        Map<String, File> artifactsMap = artifactsParam;

        try {
            List<String> uploadedKeys = dependencyUploader.uploadArtifacts(artifactsMap);

            if (expectedOverallExceptionType != null) {
                fail("Prevista eccezione di tipo " + expectedOverallExceptionType.getSimpleName() + " ma nessuna eccezione è stata lanciata.");
            }

            assertNotNull("La lista di chiavi non dovrebbe essere null per upload di successo", uploadedKeys);
            assertEquals("Il numero di chiavi caricate non corrisponde al numero di artefatti", artifactsMap.size(), uploadedKeys.size());


        } catch (Throwable caughtException) {
            if (expectedOverallExceptionType == null) {
                fail("Nessuna eccezione prevista, ma è stata catturata: " + caughtException.getClass().getSimpleName() + " con messaggio: " + caughtException.getMessage());
            }
            assertTrue("Tipo di eccezione atteso " + expectedOverallExceptionType.getSimpleName() +
                            " ma catturato " + caughtException.getClass().getSimpleName(),
                    expectedOverallExceptionType.isInstance(caughtException));


        }
        System.out.println("Parametro uploadArtifacts passato: " + artifactsParam);
    }
}