package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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

    @After
    public void teardown() {
        // eventual shutdown
    }
    @Parameterized.Parameters(name = "{index}: {3}")
    public static Object[][] data() {
        return new Object[][]{
                {Arrays.asList("blob1","blob2"), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Due blob validi"},
                {List.of(), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Lista vuota"},
                {null, TestResult.NULL_POINTER_EXCEPTION, Collections.emptyMap(), "Lista null"},
                {Collections.singletonList((String) null), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Blob null gestito"},
                {Arrays.asList("blob1", null, "blob2"), TestResult.NO_EXCEPTION, Collections.emptyMap(), "Blob misti con null"},
                {Arrays.asList("failKey","successKey"), TestResult.NO_EXCEPTION, Map.of("failKey", new RuntimeException("Simulato fallimento")), "Blob fallisce parzialmente"}
        };
    }


    @Test
    public void deleteBlobsTest() {
        System.out.println("Eseguendo test: " + testDescription);

        // Configura i throw del mock
        blobStoreThrowables.forEach((k, ex) -> {
            if (k == null) {
                try {
                    doThrow(ex).when(mockBlobStore).deleteBlob(isNull());
                } catch (AuthorizationException | KeyNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    doThrow(ex).when(mockBlobStore).deleteBlob(eq(k));
                } catch (AuthorizationException | KeyNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            dependencyUploader.deleteBlobs(keysParam);

            assertEquals(TestResult.NO_EXCEPTION, expectedOverallResult);

            if (keysParam != null) {
                for (String k : keysParam) {
                    if (k == null) verify(mockBlobStore).deleteBlob(isNull());
                    else verify(mockBlobStore).deleteBlob(eq(k));
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
