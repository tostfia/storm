package randoop.spout;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ErrorTest0 {

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
    public void test01() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test01");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) "$checkpointspout");
    }

    @Test
    public void test02() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test02");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test03() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test03");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.fail((java.lang.Object) "action");
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout3);
    }

    @Test
    public void test04() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test04");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test05() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test05");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test06() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test06");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) '4');
    }

    @Test
    public void test07() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test07");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test08() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test08");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test09() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test09");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) '4');
    }

    @Test
    public void test10() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test10");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test11() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test11");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass7 = checkpointSpout6.getClass();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
    }

    @Test
    public void test12() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test12");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) 100.0f);
    }

    @Test
    public void test13() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test13");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test14() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test14");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test15() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test15");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test16() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test16");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.fail((java.lang.Object) "action");
        checkpointSpout7.activate();
        checkpointSpout7.deactivate();
        checkpointSpout7.fail((java.lang.Object) ' ');
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) ' ');
    }

    @Test
    public void test17() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test17");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) 100.0d);
    }

    @Test
    public void test18() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test18");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test19() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test19");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout7);
    }

    @Test
    public void test20() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test20");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test21() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test21");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) 1.0d);
    }

    @Test
    public void test22() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test22");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test23() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test23");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) '4');
    }

    @Test
    public void test24() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test24");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test25() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test25");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test26() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test26");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) false);
    }

    @Test
    public void test27() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test27");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test28() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test28");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test29() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test29");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test30() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test30");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test31() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test31");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test32() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test32");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test33() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test33");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test34() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test34");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.close();
        checkpointSpout6.deactivate();
        checkpointSpout6.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
    }

    @Test
    public void test35() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test35");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test36() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test36");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) "action");
    }

    @Test
    public void test37() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test37");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.fail((java.lang.Object) "action");
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        checkpointSpout4.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout4);
    }

    @Test
    public void test38() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test38");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) (byte) 1);
    }

    @Test
    public void test39() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test39");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.fail((java.lang.Object) "action");
        checkpointSpout5.activate();
        checkpointSpout5.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout5.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test40() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test40");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test41() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test41");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test42() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test42");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout5.deactivate();
        checkpointSpout5.activate();
        checkpointSpout5.close();
        checkpointSpout5.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout5);
    }

    @Test
    public void test43() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test43");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test44() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test44");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test45() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test45");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test46() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test46");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test47() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test47");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test48() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test48");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test49() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test49");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test50() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test50");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test51() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test51");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 100.0d);
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) false);
    }

    @Test
    public void test52() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test52");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test53() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test53");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test54() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test54");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test55() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test55");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test56() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test56");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.fail((java.lang.Object) "action");
        checkpointSpout9.activate();
        checkpointSpout9.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.fail((java.lang.Object) "action");
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        checkpointSpout22.fail((java.lang.Object) (byte) 1);
        checkpointSpout22.close();
        checkpointSpout22.activate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout22);
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout9.ack((java.lang.Object) checkpointSpout18);
    }

    @Test
    public void test57() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test57");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test58() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test58");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.fail((java.lang.Object) "action");
        checkpointSpout5.activate();
        checkpointSpout5.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout5.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test59() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test59");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test60() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test60");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.fail((java.lang.Object) "action");
        checkpointSpout3.activate();
        checkpointSpout3.close();
        checkpointSpout3.deactivate();
        checkpointSpout3.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.activate();
        java.lang.Class<?> wildcardClass17 = checkpointSpout10.getClass();
        checkpointSpout3.fail((java.lang.Object) wildcardClass17);
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout3.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout3);
    }

    @Test
    public void test61() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test61");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.fail((java.lang.Object) "action");
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout9.fail((java.lang.Object) (byte) 1);
        checkpointSpout9.close();
        checkpointSpout9.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout9);
    }

    @Test
    public void test62() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test62");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout11);
    }

    @Test
    public void test63() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test63");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
    }

    @Test
    public void test64() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test64");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.fail((java.lang.Object) "action");
        checkpointSpout2.activate();
        checkpointSpout2.close();
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.fail((java.lang.Object) "action");
        checkpointSpout9.activate();
        checkpointSpout9.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.activate();
        java.lang.Class<?> wildcardClass16 = checkpointSpout9.getClass();
        checkpointSpout2.fail((java.lang.Object) wildcardClass16);
        checkpointSpout0.fail((java.lang.Object) wildcardClass16);
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test65() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test65");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test66() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test66");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.deactivate();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.close();
        checkpointSpout10.fail((java.lang.Object) 10.0d);
        checkpointSpout10.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout10);
    }

    @Test
    public void test67() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test67");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.close();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        checkpointSpout11.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout11);
    }

    @Test
    public void test68() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test68");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.fail((java.lang.Object) "action");
        checkpointSpout9.activate();
        checkpointSpout9.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) (short) -1);
    }

    @Test
    public void test69() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test69");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test70() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test70");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test71() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test71");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout2);
    }

    @Test
    public void test72() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test72");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test73() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test73");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test74() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test74");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test75() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test75");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test76() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test76");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test77() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test77");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test78() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test78");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test79() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test79");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.fail((java.lang.Object) "action");
        checkpointSpout5.activate();
        checkpointSpout5.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout5.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.deactivate();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        checkpointSpout14.close();
        checkpointSpout14.fail((java.lang.Object) 10.0d);
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) 10.0d);
    }

    @Test
    public void test80() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test80");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test81() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test81");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test82() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test82");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test83() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test83");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test84() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test84");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test85() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test85");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout10);
    }

    @Test
    public void test86() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test86");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test87() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test87");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test88() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test88");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test89() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test89");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.fail((java.lang.Object) "action");
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        checkpointSpout4.fail((java.lang.Object) (byte) 1);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout4.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout4.nextTuple();
    }

    @Test
    public void test90() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test90");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test91() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test91");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test92() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test92");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test93() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test93");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test94() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test94");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.fail((java.lang.Object) "action");
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (byte) 1);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
    }

    @Test
    public void test95() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test95");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.fail((java.lang.Object) "action");
        checkpointSpout7.activate();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.activate();
        java.lang.Class<?> wildcardClass14 = checkpointSpout7.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        checkpointSpout16.close();
        java.lang.Class<?> wildcardClass26 = checkpointSpout16.getClass();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
    }

    @Test
    public void test96() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test96");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test97() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test97");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test98() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test98");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }

    @Test
    public void test99() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "ErrorTest0.test99");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.deactivate();
        // during test generation this statement threw an exception of type java.lang.NullPointerException in error
        checkpointSpout0.nextTuple();
    }
}

