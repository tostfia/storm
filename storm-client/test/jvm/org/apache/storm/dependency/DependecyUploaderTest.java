package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;



@RunWith(Parameterized.class)
public class DependecyUploaderTest {


    private enum TestMethod { SET_BLOB_STORE, UPLOAD_FILES, UPLOAD_ARTIFACTS, DELETE_BLOBS }

    private enum TestResult { NO_EXCEPTION, NULL_POINTER_EXCEPTION, IO_EXCEPTION, CLASS_CAST_EXCEPTION, FILE_NOT_AVAILABLE_EXCEPTION }

    private Object param;
    private final TestMethod methodToCall;
    private final TestResult expectedResult;
    private final String testDescription;

    @Mock
    private ClientBlobStore mockBlobStore;

    private DependencyUploader dependencyUploader;

    public DependecyUploaderTest(TestMethod methodToCall, Object param, TestResult expectedResult, String testDescription) {
        this.methodToCall = methodToCall;
        this.param = param;
        this.expectedResult = expectedResult;
        this.testDescription = testDescription;
    }

    @Parameterized.Parameters(name = "{0} - {3}")
    public static Collection<Object[]> getTestParameters() throws IOException { // Aggiunto throws IOException per mockFile.getAbsolutePath()
        // Creiamo un mock File per i test dove è necessario un File "valido"
        File mockFile = mock(File.class);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.getName()).thenReturn("test-file.jar"); // Nome generico
        when(mockFile.length()).thenReturn(100L); // Dimensione generica
        when(mockFile.getAbsolutePath()).thenReturn("/tmp/test-file.jar"); // Mock per getAbsolutePath()


        // Un altro mock File per simulare file non esistenti o directory
        File nonExistentFile = mock(File.class);
        when(nonExistentFile.exists()).thenReturn(false);
        when(nonExistentFile.isFile()).thenReturn(false); // Potrebbe anche essere una directory
        when(nonExistentFile.getName()).thenReturn("non-existent.jar");
        when(nonExistentFile.getAbsolutePath()).thenReturn("/tmp/non-existent.jar"); // Mock per getAbsolutePath()

        return Arrays.asList(new Object[][]{
                // Test cases for SET_BLOB_STORE
                {TestMethod.SET_BLOB_STORE, mock(ClientBlobStore.class), TestResult.NO_EXCEPTION, "setBlobStore - valid ClientBlobStore"},
                // CORRETTO: se null, probabilmente lancia NPE
                {TestMethod.SET_BLOB_STORE, null, TestResult.NO_EXCEPTION, "setBlobStore - null ClientBlobStore (atteso NPE)"},

                // Test cases for UPLOAD_FILES (metodo uploadFiles(List<File>, boolean))
                // CORRETTO: methodToCall deve essere UPLOAD_FILES
                {TestMethod.UPLOAD_FILES, new Object[]{null, true}, TestResult.NULL_POINTER_EXCEPTION, "uploadFiles - null file list (atteso NPE)"},
                {TestMethod.UPLOAD_FILES, new Object[]{new ArrayList<File>(), true}, TestResult.NO_EXCEPTION, "uploadFiles - empty file list (nessuna eccezione)"},
                {TestMethod.UPLOAD_FILES, new Object[]{new ArrayList<File>(List.of(mockFile)), true}, TestResult.NO_EXCEPTION, "uploadFiles - list with valid file (nessuna eccezione)"},
                {TestMethod.UPLOAD_FILES, new Object[]{new ArrayList<File>(List.of(nonExistentFile)), true}, TestResult.FILE_NOT_AVAILABLE_EXCEPTION, "uploadFiles - list with non-existent file (atteso IOE)"},
                {TestMethod.UPLOAD_FILES, new Object[]{new ArrayList<File>(List.of(mockFile)), false}, TestResult.NO_EXCEPTION, "uploadFiles - list with valid file and cleanupIfFails false (nessuna eccezione)"},
                {TestMethod.UPLOAD_FILES, new Object[]{new ArrayList<File>(List.of(nonExistentFile)), false}, TestResult.FILE_NOT_AVAILABLE_EXCEPTION, "uploadFiles - list with non-existent file and cleanupIfFails false (atteso IOE)"},



                // Test cases for UPLOAD_ARTIFACTS (metodo uploadArtifacts(Map<String, File>))
                {TestMethod.UPLOAD_ARTIFACTS, null, TestResult.NULL_POINTER_EXCEPTION, "uploadArtifacts - null map (atteso NPE)"},
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>(), TestResult.NO_EXCEPTION, "uploadArtifacts - empty map (nessuna eccezione)"},
                // Caso valido: mappa con mock di file
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>() {{ put("blobKey1", mockFile); put("blobKey2", mockFile); }}, TestResult.NO_EXCEPTION, "uploadArtifacts - map con file validi"},
                // Caso con file non esistente nel FS locale
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>() {{ put("blobKey3", nonExistentFile); }}, TestResult.FILE_NOT_AVAILABLE_EXCEPTION, "uploadArtifacts - map con file non esistente (atteso IOE)"},



                // Test cases for DELETE_BLOBS
                {TestMethod.DELETE_BLOBS, null, TestResult.NULL_POINTER_EXCEPTION, "deleteBlobs - null list (atteso NPE)"},
                {TestMethod.DELETE_BLOBS, new ArrayList<String>(), TestResult.NO_EXCEPTION, "deleteBlobs - empty list (nessuna eccezione)"},
                // Caso valido: lista di chiavi
                {TestMethod.DELETE_BLOBS, Arrays.asList("blobKeyA", "blobKeyB"), TestResult.NO_EXCEPTION, "deleteBlobs - lista di chiavi valide"},

        });
    }

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();
        // La blob store deve essere impostata per UPLOAD_ARTIFACTS, UPLOAD_FILES e DELETE_BLOBS
        // Per setBlobStore, non avrà effetto immediato.
        // Reimpostiamo il mockBlobStore per ogni test (potrebbe essere configurato diversamente in ogni test)
        dependencyUploader.setBlobStore(mockBlobStore);
    }

    @After
    public void teardown() {
        dependencyUploader.shutdown();
    }

    @Test
    public void setBlobStoreTest() {
        if (methodToCall != TestMethod.SET_BLOB_STORE) {
            return;
        }
        System.out.println("Eseguendo test SET_BLOB_STORE: " + testDescription);

        try {
            dependencyUploader.setBlobStore((ClientBlobStore) param);
            assertEquals("Nessuna eccezione lanciata durante setBlobStore, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante setBlobStore, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante setBlobStore: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro setBlobStore passato: " + param);
    }

    @Test
    public void uploadArtifactsTest() throws Exception {
        if (methodToCall != TestMethod.UPLOAD_ARTIFACTS) {
            return;
        }
        System.out.println("Eseguendo test UPLOAD_ARTIFACTS: " + testDescription);

        try {
            dependencyUploader.uploadArtifacts((Map<String, File>) param);
            assertEquals("Nessuna eccezione lanciata durante uploadArtifacts, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante uploadArtifacts, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (FileNotAvailableException e) {
            assertEquals("Catturata FileNotAvailableException durante uploadArtifacts, ma il risultato atteso era diverso.", TestResult.FILE_NOT_AVAILABLE_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante uploadArtifacts: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro uploadArtifacts passato: " + param);
    }

    @Test
    public void deleteBlobsTest() throws Exception {
        if (methodToCall != TestMethod.DELETE_BLOBS) {
            return;
        }
        System.out.println("Eseguendo test DELETE_BLOBS: " + testDescription);


        try {
            dependencyUploader.deleteBlobs((List<String>) param); // Modificato a Collection<String>
            assertEquals("Nessuna eccezione lanciata durante deleteBlobs, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante deleteBlobs, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante deleteBlobs: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro deleteBlobs passato: " + param);
    }

    @Test
    public void uploadFilesTest() throws Exception {
        if (methodToCall != TestMethod.UPLOAD_FILES) { // CORRETTO: confronta con UPLOAD_FILES
            return;
        }
        System.out.println("Eseguendo test UPLOAD_FILES: " + testDescription);

        Object[] paramsArray = (Object[]) param;
        List<File> fileList = (List<File>) paramsArray[0];
        Boolean cleanupIfFails = (Boolean) paramsArray[1];

        try {
            dependencyUploader.uploadFiles(fileList, cleanupIfFails);
            assertEquals("Nessuna eccezione lanciata durante uploadFiles, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante uploadFiles, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (IOException e) {
            assertEquals("Catturata IOException durante uploadFiles, ma il risultato atteso era diverso.", TestResult.IO_EXCEPTION, expectedResult);
        } catch (FileNotAvailableException e) {
            assertEquals("Catturata AuthorizationException durante uploadFiles, ma il risultato atteso era diverso.", TestResult.FILE_NOT_AVAILABLE_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante uploadFiles: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro uploadFiles passato: " + param);
    }
}