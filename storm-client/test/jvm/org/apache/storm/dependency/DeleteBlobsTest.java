package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DeleteBlobsTest {

    public enum TestResult { NO_EXCEPTION, NULL_POINTER_EXCEPTION }

    @Parameterized.Parameter(0)
    public List<String> keysParam;

    @Parameterized.Parameter(1)
    public TestResult expectedOverallResult;

    @Parameterized.Parameter(2)
    public Map<String, Throwable> blobStoreThrowables;

    @Parameterized.Parameter(3)
    public String testDescription;

    @Mock
    private ClientBlobStore mockBlobStore;

    private DependencyUploader dependencyUploader;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        dependencyUploader = new DependencyUploader();
        dependencyUploader.setBlobStore(mockBlobStore);
    }



    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {null, TestResult.NULL_POINTER_EXCEPTION, Collections.emptyMap(), "Lista null"},
                {Collections.emptyList(), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Lista vuota"},
                {Arrays.asList("blob1","blob2"), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Due blob validi"},
                {Collections.singletonList((String) null), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Blob null gestito"},
                {Arrays.asList("blob1", null, "blob2"), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Blob misti con null"},
                {Arrays.asList("ok1","fail1","ok2", null, "fail2"),
                        TestResult.NO_EXCEPTION,
                        Map.of(
                                "fail1", new RuntimeException("Simulato fail1"),
                                "fail2", new RuntimeException("Simulato fail2")
                        ),
                        "Blob con eccezioni parziali, ciclo continua"}
        });
    }

    @Test
    public void deleteBlobsTest() throws Exception {
        System.out.println("Eseguendo test: " + testDescription);

        // Configura eccezioni sui mock
        if (blobStoreThrowables != null) {
            blobStoreThrowables.forEach((key, ex) -> {
                try {
                    if (key == null) {
                        doThrow(ex).when(mockBlobStore).deleteBlob(isNull());
                    } else {
                        doThrow(ex).when(mockBlobStore).deleteBlob(eq(key));
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        }

        try {
            dependencyUploader.deleteBlobs(keysParam);

            assertEquals(TestResult.NO_EXCEPTION, expectedOverallResult);

            // Verifica che deleteBlob sia stato chiamato per tutte le chiavi (anche null)
            if (keysParam != null) {
                for (String key : keysParam) {
                    verify(mockBlobStore).deleteBlob(key);
                }
            } else {
                verifyNoInteractions(mockBlobStore);
            }

        } catch (NullPointerException e) {
            assertEquals(TestResult.NULL_POINTER_EXCEPTION, expectedOverallResult);
            verifyNoInteractions(mockBlobStore);
        } catch (Throwable e) {
            fail("Eccezione inattesa: " + e);
        }
    }
}
