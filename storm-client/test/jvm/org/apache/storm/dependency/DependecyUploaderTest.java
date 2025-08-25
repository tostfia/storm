package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
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

    private enum TestMethod { SET_BLOB_STORE, UPLOAD_ARTIFACTS, DELETE_BLOBS }
    private enum TestResult { NO_EXCEPTION, NULL_POINTER_EXCEPTION, IO_EXCEPTION, KEY_ALREADY_EXISTS_EXCEPTION, KEY_NOT_FOUND_EXCEPTION, AUTHORIZATION_EXCEPTION }

    private Object param;
    private final TestMethod methodToCall;
    private final TestResult expectedResult;
    private final String testDescription;

    @Mock
    private ClientBlobStore mockBlobStore;

    private DependencyUploader dependencyUploader;

    // Il costruttore corrisponde all'ordine dei parametri in @Parameterized.Parameters
    public DependecyUploaderTest(TestMethod methodToCall, Object param, TestResult expectedResult, String testDescription) {
        this.methodToCall = methodToCall;
        this.param = param;
        this.expectedResult = expectedResult;
        this.testDescription = testDescription;
    }

    @Parameterized.Parameters(name = "{0} - {3}")
    public static Collection<Object[]> getTestParameters() {
        // Creiamo un mock File per i test dove è necessario un File "valido"
        File mockFile = mock(File.class);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.getName()).thenReturn("test-file.jar"); // Nome generico
        when(mockFile.length()).thenReturn(100L); // Dimensione generica
        try {
            when(mockFile.getAbsolutePath()).thenReturn("/tmp/test-file.jar");
        } catch (Exception e) {
            // Non dovrebbe succedere in un mock
        }


        // Un altro mock File per simulare file non esistenti o directory
        File nonExistentFile = mock(File.class);
        when(nonExistentFile.exists()).thenReturn(false);
        when(nonExistentFile.isFile()).thenReturn(false); // Potrebbe anche essere una directory
        when(nonExistentFile.getName()).thenReturn("non-existent.jar");
        try {
            when(nonExistentFile.getAbsolutePath()).thenReturn("/tmp/non-existent.jar");
        } catch (Exception e) {}

        return Arrays.asList(new Object[][]{
                // Test cases for SET_BLOB_STORE
                {TestMethod.SET_BLOB_STORE, mock(ClientBlobStore.class), TestResult.NO_EXCEPTION, "setBlobStore - valid ClientBlobStore"},
                {TestMethod.SET_BLOB_STORE, null, TestResult.NULL_POINTER_EXCEPTION, "setBlobStore - null ClientBlobStore (atteso NPE)"},

                // Test cases for UPLOAD_ARTIFACTS
                {TestMethod.UPLOAD_ARTIFACTS, null, TestResult.NULL_POINTER_EXCEPTION, "uploadArtifacts - null map (atteso NPE)"},
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>(), TestResult.NO_EXCEPTION, "uploadArtifacts - empty map (nessuna eccezione)"},
                // Caso valido: mappa con mock di file
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>() {{ put("blobKey1", mockFile); put("blobKey2", mockFile); }}, TestResult.NO_EXCEPTION, "uploadArtifacts - map con file validi"},
                // Caso con file non esistente nel FS locale
                {TestMethod.UPLOAD_ARTIFACTS, new HashMap<String, File>() {{ put("blobKey3", nonExistentFile); }}, TestResult.IO_EXCEPTION, "uploadArtifacts - map con file non esistente (atteso IOE)"},
                // Caso con ClientBlobStore che lancia KeyAlreadyExistsException
                {TestMethod.UPLOAD_ARTIFACTS, "THROW_KEY_ALREADY_EXISTS", TestResult.KEY_ALREADY_EXISTS_EXCEPTION, "uploadArtifacts - BlobStore lancia KeyAlreadyExistsException"},
                // Caso con ClientBlobStore che lancia AuthorizationException
                {TestMethod.UPLOAD_ARTIFACTS, "THROW_AUTHORIZATION_EXCEPTION", TestResult.AUTHORIZATION_EXCEPTION, "uploadArtifacts - BlobStore lancia AuthorizationException (createBlob)"},
                // Caso con ClientBlobStore che lancia IOException durante la scrittura
                {TestMethod.UPLOAD_ARTIFACTS, "THROW_IO_EXCEPTION_WRITE", TestResult.IO_EXCEPTION, "uploadArtifacts - BlobStore lancia IOException (writeBlob)"},


                // Test cases for DELETE_BLOBS
                {TestMethod.DELETE_BLOBS, null, TestResult.NULL_POINTER_EXCEPTION, "deleteBlobs - null list (atteso NPE)"},
                {TestMethod.DELETE_BLOBS, new ArrayList<String>(), TestResult.NO_EXCEPTION, "deleteBlobs - empty list (nessuna eccezione)"},
                // Caso valido: lista di chiavi
                {TestMethod.DELETE_BLOBS, Arrays.asList("blobKeyA", "blobKeyB"), TestResult.NO_EXCEPTION, "deleteBlobs - lista di chiavi valide"},
                // Caso con ClientBlobStore che lancia KeyNotFoundException
                {TestMethod.DELETE_BLOBS, "THROW_KEY_NOT_FOUND", TestResult.KEY_NOT_FOUND_EXCEPTION, "deleteBlobs - BlobStore lancia KeyNotFoundException"},
                // Caso con ClientBlobStore che lancia AuthorizationException
                {TestMethod.DELETE_BLOBS, "THROW_AUTHORIZATION_EXCEPTION", TestResult.AUTHORIZATION_EXCEPTION, "deleteBlobs - BlobStore lancia AuthorizationException (deleteBlob)"},
                // Caso con ClientBlobStore che lancia IOException
                {TestMethod.DELETE_BLOBS, "THROW_IO_EXCEPTION_DELETE", TestResult.IO_EXCEPTION, "deleteBlobs - BlobStore lancia IOException (deleteBlob)"},
        });
    }

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();
        // Per i test di UPLOAD_ARTIFACTS e DELETE_BLOBS, la blob store deve essere impostata
        // La impostiamo qui di default, ma per i test di setBlobStore non avrà effetto immediato
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
    public void uploadArtifactsTest() throws Exception { // Aggiunto 'throws Exception' per mockito.when()
        if (methodToCall != TestMethod.UPLOAD_ARTIFACTS) {
            return;
        }
        System.out.println("Eseguendo test UPLOAD_ARTIFACTS: " + testDescription);

        // Configura il mockBlobStore per i casi di eccezione specifici
        /*if (param instanceof String) {
            String specialCase = (String) param;
            if (specialCase.equals("THROW_KEY_ALREADY_EXISTS")) {
                doThrow(new KeyAlreadyExistsException("Test Key Already Exists")).when(mockBlobStore).createBlob(anyString(), any(), any());
                // Non importa quali File mettiamo nella mappa, l'eccezione verrà lanciata da createBlob
                param = new HashMap<String, File>() {{ put("testKey", mock(File.class)); }};
            } else if (specialCase.equals("THROW_AUTHORIZATION_EXCEPTION")) {
                doThrow(new AuthorizationException("Test Authorization Exception")).when(mockBlobStore).createBlob(anyString(), any(), any());
                param = new HashMap<String, File>() {{ put("testKey", mock(File.class)); }};
            } else if (specialCase.equals("THROW_IO_EXCEPTION_WRITE")) {
                // Mockare la scrittura su blob. Per un test più dettagliato, potresti mockare BlobOutputStream
                doThrow(new IOException("Test IO Exception on write")).when(mockBlobStore).writeBlob(anyString(), any());
                File mockFile = mock(File.class);
                when(mockFile.exists()).thenReturn(true);
                when(mockFile.isFile()).thenReturn(true);
                when(mockFile.getName()).thenReturn("write-fail.jar");
                when(mockFile.length()).thenReturn(100L);
                when(mockFile.getAbsolutePath()).thenReturn("/tmp/write-fail.jar");
                param = new HashMap<String, File>() {{ put("testKey", mockFile); }};
            }
        }


        try {

            dependencyUploader.uploadArtifacts((Map<String, File>) param);
            assertEquals("Nessuna eccezione lanciata durante uploadArtifacts, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante uploadArtifacts, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante uploadArtifacts: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro uploadArtifacts passato: " + param);*/
    }

    @Test
    public void deleteBlobsTest() throws Exception { // Aggiunto 'throws Exception' per mockito.when()
        if (methodToCall != TestMethod.DELETE_BLOBS) {
            return;
        }
        System.out.println("Eseguendo test DELETE_BLOBS: " + testDescription);

        // Configura il mockBlobStore per i casi di eccezione specifici
        if (param instanceof String specialCase) {
            switch (specialCase) {
                case "THROW_KEY_NOT_FOUND" -> {
                    doThrow(new KeyNotFoundException("Test Key Not Found")).when(mockBlobStore).deleteBlob(anyString());
                    param = Collections.singletonList("nonExistentKey"); // Fornisci una lista per la chiamata al metodo
                }
                case "THROW_AUTHORIZATION_EXCEPTION" -> {
                    doThrow(new AuthorizationException("Test Authorization Exception")).when(mockBlobStore).deleteBlob(anyString());
                    param = Collections.singletonList("unauthorizedKey");
                }
                case "THROW_IO_EXCEPTION_DELETE" -> {
                    doThrow(new IOException("Test IO Exception on delete")).when(mockBlobStore).deleteBlob(anyString());
                    param = Collections.singletonList("ioErrorKey");
                }
            }
        }


        try {
            dependencyUploader.deleteBlobs((List<String>) param);
            assertEquals("Nessuna eccezione lanciata durante deleteBlobs, ma ci si aspettava un'eccezione.", TestResult.NO_EXCEPTION, expectedResult);
        } catch (NullPointerException e) {
            assertEquals("Catturata NullPointerException durante deleteBlobs, ma il risultato atteso era diverso.", TestResult.NULL_POINTER_EXCEPTION, expectedResult);
        } catch (Exception e) {
            fail("Catturata un'eccezione inattesa durante deleteBlobs: " + e.getClass().getSimpleName() + " con messaggio: " + e.getMessage());
        }
        System.out.println("Parametro deleteBlobs passato: " + param);
    }
}