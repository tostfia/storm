package org.apache.storm.dependency;


import org.junit.Test;

public class InitTestLlm {

    @Test
    public void testInitDoesNothing() {
        DependencyUploader uploader = new DependencyUploader();
        uploader.init(); // NOOP, just ensure no exception
    }
}