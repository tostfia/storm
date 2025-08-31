package org.apache.storm.dependency;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class DependencyUploaderInitShutdownTest {

    private DependencyUploader uploader;

    @Before
    public void setUp() {
        uploader = new DependencyUploader();
    }

    @Test
    public void testInitAllowsUpload() throws Exception {
        uploader.init();
        // Se init funziona, uploadFiles() dovrebbe eseguire senza eccezioni
        uploader.uploadFiles(Collections.<File>emptyList(),true);
    }



    @Test
    public void testDoubleInitIsSafe() {
        uploader.init();
        uploader.init(); // non deve sollevare errori
    }

    @Test
    public void testShutdownWithoutInitDoesNotThrow() {
        uploader.shutdown(); // dovrebbe completarsi senza eccezioni
    }

    @Test
    public void testDoubleShutdownIsSafe() {
        uploader.init();
        uploader.shutdown();
        uploader.shutdown(); // seconda chiamata sicura
    }
}
