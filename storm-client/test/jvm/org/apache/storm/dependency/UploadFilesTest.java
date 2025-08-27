package org.apache.storm.dependency;



import org.apache.storm.generated.AuthorizationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.mockito.MockitoAnnotations;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class UploadFilesTest {

    private final Object[] paramsArray; // Contiene {List<File>, Boolean}
    private final Class<? extends Throwable> expectedOverallExceptionType;
    private final String testDescription;
    private final Map<String, Boolean> fileExistsStatus; // Per simulare File.exists()




    private DependencyUploader dependencyUploader;

    public UploadFilesTest(Object[] paramsArray, Class<? extends Throwable> expectedOverallExceptionType, String testDescription,
                           Map<String, Boolean> fileExistsStatus) {
        this.paramsArray = paramsArray;
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
            // by default exists() -> true (puoi controllare via mappe nei test)
            when(mockFile.exists()).thenReturn(true);
            return mockFile;
        };

        // File simulati (deterministici, non temp casuali)
        File file1 = createMockFile.apply("test1.txt");
        File file2 = createMockFile.apply("test2.txt");
        File file_nonExistent = createMockFile.apply("non_existent.txt");
        File file_emptyName = createMockFile.apply("");
        File file_longName = createMockFile.apply("a".repeat(200) + ".txt");
        File file_specialChars = createMockFile.apply("file-with_chars!.txt");
        File file_uploadFail_IO = createMockFile.apply("upload_fail_io.txt");
        File file_uploadFail_Auth = createMockFile.apply("upload_fail_auth.txt");
        File file_keyExists = createMockFile.apply("key_exists.txt");
        File file_blobStoreNull = createMockFile.apply("blobstore_null.txt");

        // Mappa "tutti i file esistono"
        Map<String, Boolean> allFilesExist = Map.ofEntries(
                Map.entry(file1.getName(), true),
                Map.entry(file2.getName(), true),
                Map.entry(file_emptyName.getName(), true),
                Map.entry(file_longName.getName(), true),
                Map.entry(file_specialChars.getName(), true),
                Map.entry(file_uploadFail_IO.getName(), true),
                Map.entry(file_uploadFail_Auth.getName(), true),
                Map.entry(file_keyExists.getName(), true),
                Map.entry(file_blobStoreNull.getName(), true)
        );

        // Variante con file_nonExistent che non esiste
        Map<String, Boolean> fileNotExistsSpecific = new HashMap<>(allFilesExist);
        fileNotExistsSpecific.put(file_nonExistent.getName(), false);

        return Arrays.asList(new Object[][]{
                // Parametri: { input, eccezione attesa, descrizione, fileExistsMap}

                {new Object[]{null, true}, NullPointerException.class,
                        "uploadFiles - lista di input null", Collections.emptyMap()},//0

                {new Object[]{new ArrayList<File>(), true}, null,
                        "uploadFiles - lista vuota", Collections.emptyMap()},//1

                {new Object[]{List.of(file1), true}, FileNotAvailableException.class,
                        "uploadFiles - singolo file valido", allFilesExist},//2

                {new Object[]{List.of(file1, file2), true}, FileNotAvailableException.class,
                        "uploadFiles - più file validi", allFilesExist},//3

                {new Object[]{new ArrayList<>(Arrays.asList(file1, null, file2)), true}, FileNotAvailableException.class,
                        "uploadFiles - lista con file null", allFilesExist},//4

                {new Object[]{List.of(file_nonExistent), true}, FileNotAvailableException.class,
                        "uploadFiles - file non esistente (cleanup=true)", fileNotExistsSpecific},//5

                {new Object[]{List.of(file_nonExistent), false}, FileNotAvailableException.class,
                        "uploadFiles - file non esistente (cleanup=false)", fileNotExistsSpecific},//6

                {new Object[]{List.of(file_uploadFail_IO), true}, FileNotAvailableException.class,
                        "uploadFiles - upload fallisce (IO), cleanup=true", allFilesExist},//7

                {new Object[]{List.of(file_uploadFail_Auth), true}, FileNotAvailableException.class,
                        "uploadFiles - upload fallisce (Auth), cleanup=true", allFilesExist},

                {new Object[]{List.of(file_keyExists), true}, RuntimeException.class,
                        "uploadFiles - KeyAlreadyExistsException (incapsulata), cleanup=true", allFilesExist},

                {new Object[]{List.of(file1, file_uploadFail_IO, file2), true}, FileNotAvailableException.class,
                        "uploadFiles - più file, uno fallisce in mezzo, cleanup=true", allFilesExist},

                // BlobStore non configurato
                {new Object[]{List.of(file_blobStoreNull), true}, FileNotAvailableException.class,
                        "uploadFiles - BlobStore null durante upload, cleanup=true",allFilesExist},

                // Altri casi limite
                {new Object[]{List.of(file_emptyName), true}, FileNotAvailableException.class,
                        "uploadFiles - nome file vuoto", allFilesExist},

                {new Object[]{List.of(file_longName), true}, FileNotAvailableException.class,
                        "uploadFiles - nome file molto lungo", allFilesExist},

                {new Object[]{List.of(file_specialChars), true}, FileNotAvailableException.class,
                        "uploadFiles - nome file con caratteri speciali", allFilesExist},

                {new Object[]{List.of(file_nonExistent, file_nonExistent), true}, FileNotAvailableException.class,
                        "uploadFiles - due file non esistenti", fileNotExistsSpecific},

                {new Object[]{List.of(file1, file_uploadFail_IO), false}, FileNotAvailableException.class,
                        "uploadFiles - upload fallisce (IO), cleanup=false", allFilesExist},
        });
    }

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();



        // Configura il comportamento di File.exists() per i mock File
        List<File> fileList = (List<File>) paramsArray[0];
        if (fileList != null) {
            for (File file : fileList) {
                if (file != null) {
                    Boolean exists = fileExistsStatus.get(file.getName());
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
    public void uploadFilesTest() throws Exception {
        System.out.println("Eseguendo test UPLOAD_FILES: " + testDescription);

        List<File> fileList = (List<File>) paramsArray[0];
        Boolean cleanupIfFails = (Boolean) paramsArray[1];

        try {
            List<String> uploadedKeys = dependencyUploader.uploadFiles(fileList, cleanupIfFails);

            if (expectedOverallExceptionType != null) {
                fail("Prevista eccezione di tipo " + expectedOverallExceptionType.getSimpleName() + " ma nessuna eccezione è stata lanciata.");
            }

            assertNotNull("La lista di chiavi non dovrebbe essere null per upload di successo", uploadedKeys);
            if (fileList != null) {
                assertEquals("Il numero di chiavi caricate non corrisponde al numero di file", fileList.size(), uploadedKeys.size());
            }



        } catch (Throwable caughtException) {
            if (expectedOverallExceptionType == null) {
                fail("Nessuna eccezione prevista, ma è stata catturata: " + caughtException.getClass().getSimpleName() + " con messaggio: " + caughtException.getMessage());
            }
            assertTrue("Tipo di eccezione atteso " + expectedOverallExceptionType.getSimpleName() +
                            " ma catturato " + caughtException.getClass().getSimpleName(),
                    expectedOverallExceptionType.isInstance(caughtException));


        }
        System.out.println("Parametro uploadFiles passato: " + Arrays.toString(paramsArray));
    }
}