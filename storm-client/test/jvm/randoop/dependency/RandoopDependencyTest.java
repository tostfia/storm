package randoop.dependency;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandoopDependencyTest {

    public static boolean debug = false;

    public void assertBooleanArrayEquals(boolean[] expectedArray, boolean[] actualArray) {
        if (expectedArray.length != actualArray.length) {
            throw new AssertionError("Array lengths differ: " + expectedArray.length + " != " + actualArray.length);
        }
        for (int i = 0; i < expectedArray.length; i++) {
            if (expectedArray[i] != actualArray[i]) {
                throw new AssertionError("Arrays differ at index " + i + ": " + expectedArray[i] + " != " + actualArray[i]);
            }
        }
    }

    @Test
    public void test1() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test1");
        org.slf4j.Logger logger0 = org.apache.storm.dependency.DependencyUploader.LOG;
        org.junit.Assert.assertNotNull(logger0);
    }

    @Test
    public void test2() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test2");
        org.apache.storm.dependency.DependencyUploader dependencyUploader0 = new org.apache.storm.dependency.DependencyUploader();
        java.lang.String[] strArray2 = new java.lang.String[] { "" };
        java.util.ArrayList<java.lang.String> strList3 = new java.util.ArrayList<java.lang.String>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList3, strArray2);
        dependencyUploader0.deleteBlobs((java.util.List<java.lang.String>) strList3);
        java.util.List<java.lang.String> strList6 = null;
        // The following exception was thrown during execution in test generation
        try {
            dependencyUploader0.deleteBlobs(strList6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.List.iterator()\" because \"keys\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(strArray2);
        org.junit.Assert.assertArrayEquals(strArray2, new java.lang.String[] { "" });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
    }

    @Test
    public void test3() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test3");
        org.apache.storm.dependency.DependencyUploader dependencyUploader0 = new org.apache.storm.dependency.DependencyUploader();
        java.io.File[] fileArray1 = new java.io.File[] {};
        java.util.ArrayList<java.io.File> fileList2 = new java.util.ArrayList<java.io.File>();
        boolean boolean3 = java.util.Collections.addAll((java.util.Collection<java.io.File>) fileList2, fileArray1);
        java.util.List<java.lang.String> strList5 = dependencyUploader0.uploadFiles((java.util.List<java.io.File>) fileList2, false);
        java.lang.Class<?> wildcardClass6 = fileList2.getClass();
        org.junit.Assert.assertNotNull(fileArray1);
        org.junit.Assert.assertArrayEquals(fileArray1, new java.io.File[] {});
        org.junit.Assert.assertTrue("'" + boolean3 + "' != '" + false + "'", boolean3 == false);
        org.junit.Assert.assertNotNull(strList5);
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test4() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test4");
        org.apache.storm.dependency.DependencyUploader dependencyUploader0 = new org.apache.storm.dependency.DependencyUploader();
        org.apache.storm.dependency.DependencyUploader dependencyUploader1 = new org.apache.storm.dependency.DependencyUploader();
        java.io.File[] fileArray2 = new java.io.File[] {};
        java.util.ArrayList<java.io.File> fileList3 = new java.util.ArrayList<java.io.File>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.io.File>) fileList3, fileArray2);
        java.util.List<java.lang.String> strList6 = dependencyUploader1.uploadFiles((java.util.List<java.io.File>) fileList3, false);
        java.util.List<java.lang.String> strList8 = dependencyUploader0.uploadFiles((java.util.List<java.io.File>) fileList3, false);
        org.junit.Assert.assertNotNull(fileArray2);
        org.junit.Assert.assertArrayEquals(fileArray2, new java.io.File[] {});
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + false + "'", boolean4 == false);
        org.junit.Assert.assertNotNull(strList6);
        org.junit.Assert.assertNotNull(strList8);
    }

    @Test
    public void test5() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test5");
        org.apache.storm.dependency.DependencyUploader dependencyUploader0 = new org.apache.storm.dependency.DependencyUploader();
        dependencyUploader0.shutdown();
        java.lang.String[] strArray4 = new java.lang.String[] { "hi!", "hi!" };
        java.util.ArrayList<java.lang.String> strList5 = new java.util.ArrayList<java.lang.String>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList5, strArray4);
        dependencyUploader0.deleteBlobs((java.util.List<java.lang.String>) strList5);
        org.junit.Assert.assertNotNull(strArray4);
        org.junit.Assert.assertArrayEquals(strArray4, new java.lang.String[] { "hi!", "hi!" });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
    }

    @Test
    public void test6() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RandoopDependencyTest.test6");
        org.apache.storm.dependency.DependencyUploader dependencyUploader0 = new org.apache.storm.dependency.DependencyUploader();
        java.lang.String[] strArray2 = new java.lang.String[] { "" };
        java.util.ArrayList<java.lang.String> strList3 = new java.util.ArrayList<java.lang.String>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList3, strArray2);
        dependencyUploader0.deleteBlobs((java.util.List<java.lang.String>) strList3);
        dependencyUploader0.shutdown();
        java.lang.Class<?> wildcardClass7 = dependencyUploader0.getClass();
        org.junit.Assert.assertNotNull(strArray2);
        org.junit.Assert.assertArrayEquals(strArray2, new java.lang.String[] { "" });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }
}

