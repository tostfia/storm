package randoop.spout;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest2 {

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
    public void test1001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1001");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass13 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1002");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        java.lang.Object obj41 = null;
        checkpointSpout38.fail(obj41);
        checkpointSpout38.close();
        checkpointSpout38.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.deactivate();
        checkpointSpout38.fail((java.lang.Object) checkpointSpout45);
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout38.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        checkpointSpout53.deactivate();
        checkpointSpout38.fail((java.lang.Object) checkpointSpout53);
        org.apache.storm.spout.CheckpointSpout checkpointSpout59 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout59.activate();
        checkpointSpout59.activate();
        java.lang.Object obj62 = null;
        checkpointSpout59.fail(obj62);
        checkpointSpout59.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout65 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout65.activate();
        checkpointSpout65.activate();
        java.lang.Object obj68 = null;
        checkpointSpout65.fail(obj68);
        checkpointSpout65.close();
        checkpointSpout65.activate();
        checkpointSpout65.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap73 = checkpointSpout65.getComponentConfiguration();
        checkpointSpout59.fail((java.lang.Object) strMap73);
        checkpointSpout59.close();
        checkpointSpout59.deactivate();
        checkpointSpout53.fail((java.lang.Object) checkpointSpout59);
        org.apache.storm.spout.CheckpointSpout checkpointSpout78 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout78.activate();
        checkpointSpout78.activate();
        java.lang.Object obj81 = null;
        checkpointSpout78.fail(obj81);
        checkpointSpout78.close();
        checkpointSpout78.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout85 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout85.activate();
        checkpointSpout85.activate();
        java.lang.Object obj88 = null;
        checkpointSpout85.fail(obj88);
        checkpointSpout85.deactivate();
        checkpointSpout78.fail((java.lang.Object) checkpointSpout85);
        checkpointSpout78.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap93 = checkpointSpout78.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap94 = checkpointSpout78.getComponentConfiguration();
        java.lang.Class<?> wildcardClass95 = checkpointSpout78.getClass();
        checkpointSpout59.fail((java.lang.Object) wildcardClass95);
        checkpointSpout16.fail((java.lang.Object) wildcardClass95);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer98 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.declareOutputFields(outputFieldsDeclarer98);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap73);
        org.junit.Assert.assertNull(strMap93);
        org.junit.Assert.assertNull(strMap94);
        org.junit.Assert.assertNotNull(wildcardClass95);
    }

    @Test
    public void test1003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1003");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = null;
        org.apache.storm.task.TopologyContext topologyContext3 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector4 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap2, topologyContext3, spoutOutputCollector4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1004");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout4.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test1005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1005");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = null;
        org.apache.storm.task.TopologyContext topologyContext20 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector21 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap19, topologyContext20, spoutOutputCollector21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test1006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1006");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout17.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout17.getComponentConfiguration();
        checkpointSpout17.activate();
        java.lang.Class<?> wildcardClass25 = checkpointSpout17.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass25);
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.fail((java.lang.Object) "action");
        checkpointSpout28.fail((java.lang.Object) "action");
        checkpointSpout0.fail((java.lang.Object) "action");
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNotNull(wildcardClass25);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test1007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1007");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test1008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1008");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1009");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) 'a');
        checkpointSpout19.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test1010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1010");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.close();
        checkpointSpout24.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.deactivate();
        checkpointSpout32.fail((java.lang.Object) checkpointSpout39);
        checkpointSpout39.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout39.getComponentConfiguration();
        checkpointSpout24.fail((java.lang.Object) strMap47);
        java.lang.Class<?> wildcardClass49 = checkpointSpout24.getClass();
        checkpointSpout8.fail((java.lang.Object) wildcardClass49);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test1011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1011");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout20.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test1012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1012");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1013");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.close();
        checkpointSpout23.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout29.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout29.fail((java.lang.Object) (-1.0d));
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test1014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1014");
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
        checkpointSpout9.activate();
        checkpointSpout9.deactivate();
        checkpointSpout9.activate();
        checkpointSpout9.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout22.activate();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        checkpointSpout37.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout31.fail((java.lang.Object) strMap45);
        checkpointSpout31.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout31);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.fail((java.lang.Object) "action");
        checkpointSpout53.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout57 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout57.activate();
        checkpointSpout57.activate();
        java.lang.Object obj60 = null;
        checkpointSpout57.fail(obj60);
        checkpointSpout57.close();
        checkpointSpout57.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout64 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout64.activate();
        checkpointSpout64.activate();
        java.lang.Object obj67 = null;
        checkpointSpout64.fail(obj67);
        checkpointSpout64.deactivate();
        checkpointSpout57.fail((java.lang.Object) checkpointSpout64);
        checkpointSpout53.fail((java.lang.Object) checkpointSpout57);
        checkpointSpout53.deactivate();
        checkpointSpout53.deactivate();
        checkpointSpout53.close();
        checkpointSpout53.close();
        java.lang.Class<?> wildcardClass76 = checkpointSpout53.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout17.ack((java.lang.Object) wildcardClass76);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNotNull(wildcardClass76);
    }

    @Test
    public void test1015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1015");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout18.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1016");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.close();
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        checkpointSpout22.activate();
        checkpointSpout22.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout16.fail((java.lang.Object) strMap30);
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout32.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout32.deactivate();
        checkpointSpout32.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout7.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout61 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout61.activate();
        checkpointSpout61.activate();
        java.lang.Object obj64 = null;
        checkpointSpout61.fail(obj64);
        checkpointSpout61.close();
        checkpointSpout61.activate();
        checkpointSpout61.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap69 = checkpointSpout61.getComponentConfiguration();
        checkpointSpout55.fail((java.lang.Object) strMap69);
        checkpointSpout55.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap72 = checkpointSpout55.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout73 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout73.activate();
        checkpointSpout73.activate();
        java.lang.Object obj76 = null;
        checkpointSpout73.fail(obj76);
        checkpointSpout73.close();
        checkpointSpout73.activate();
        checkpointSpout73.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap81 = checkpointSpout73.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap82 = checkpointSpout73.getComponentConfiguration();
        checkpointSpout55.fail((java.lang.Object) checkpointSpout73);
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.ack((java.lang.Object) checkpointSpout55);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNull(strMap69);
        org.junit.Assert.assertNull(strMap72);
        org.junit.Assert.assertNull(strMap81);
        org.junit.Assert.assertNull(strMap82);
    }

    @Test
    public void test1017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1017");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout14.getComponentConfiguration();
        checkpointSpout14.activate();
        java.lang.Class<?> wildcardClass21 = checkpointSpout14.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.fail((java.lang.Object) "action");
        checkpointSpout24.activate();
        checkpointSpout24.deactivate();
        checkpointSpout24.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.fail((java.lang.Object) "action");
        checkpointSpout31.activate();
        checkpointSpout31.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout31.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout31.getComponentConfiguration();
        checkpointSpout31.deactivate();
        checkpointSpout31.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap37);
    }

    @Test
    public void test1018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1018");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.fail((java.lang.Object) "action");
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        checkpointSpout22.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.deactivate();
        checkpointSpout22.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout18.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout12.fail((java.lang.Object) checkpointSpout22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        java.lang.Object obj41 = null;
        checkpointSpout38.fail(obj41);
        checkpointSpout38.close();
        checkpointSpout38.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.deactivate();
        checkpointSpout38.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout38.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout38.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout38.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout38.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap56 = checkpointSpout38.getComponentConfiguration();
        checkpointSpout22.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout38.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout38.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap59);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap53);
        org.junit.Assert.assertNull(strMap54);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNull(strMap56);
        org.junit.Assert.assertNull(strMap59);
    }

    @Test
    public void test1019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1019");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.fail((java.lang.Object) "action");
        checkpointSpout38.activate();
        checkpointSpout38.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout38.getComponentConfiguration();
        checkpointSpout38.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.close();
        checkpointSpout52.activate();
        checkpointSpout52.fail((java.lang.Object) 'a');
        checkpointSpout52.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout52.getComponentConfiguration();
        checkpointSpout52.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap64 = checkpointSpout52.getComponentConfiguration();
        checkpointSpout52.activate();
        checkpointSpout52.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) checkpointSpout52);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap62);
        org.junit.Assert.assertNull(strMap64);
    }

    @Test
    public void test1020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1020");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test1021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1021");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout25.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout16.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.fail((java.lang.Object) "action");
        checkpointSpout47.activate();
        checkpointSpout47.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        checkpointSpout54.close();
        checkpointSpout54.deactivate();
        checkpointSpout54.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout63 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout63.activate();
        checkpointSpout63.activate();
        java.lang.Object obj66 = null;
        checkpointSpout63.fail(obj66);
        checkpointSpout63.close();
        checkpointSpout63.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.activate();
        checkpointSpout70.activate();
        java.lang.Object obj73 = null;
        checkpointSpout70.fail(obj73);
        checkpointSpout70.deactivate();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout70);
        org.apache.storm.spout.CheckpointSpout checkpointSpout77 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout77.fail((java.lang.Object) "action");
        checkpointSpout77.activate();
        checkpointSpout77.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap82 = checkpointSpout77.getComponentConfiguration();
        checkpointSpout77.activate();
        java.lang.Class<?> wildcardClass84 = checkpointSpout77.getClass();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout77);
        checkpointSpout54.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout47.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout44.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout44.close();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout44);
        java.util.Map<java.lang.String, java.lang.Object> strMap91 = checkpointSpout44.getComponentConfiguration();
        checkpointSpout44.deactivate();
        java.lang.Class<?> wildcardClass93 = checkpointSpout44.getClass();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap82);
        org.junit.Assert.assertNotNull(wildcardClass84);
        org.junit.Assert.assertNull(strMap91);
        org.junit.Assert.assertNotNull(wildcardClass93);
    }

    @Test
    public void test1022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1022");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.close();
        checkpointSpout7.fail((java.lang.Object) 0.0f);
        checkpointSpout7.deactivate();
        checkpointSpout7.close();
        checkpointSpout7.fail((java.lang.Object) false);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
    }

    @Test
    public void test1023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1023");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        checkpointSpout32.deactivate();
        checkpointSpout32.activate();
        checkpointSpout32.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.activate();
        checkpointSpout40.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.fail((java.lang.Object) "action");
        checkpointSpout45.activate();
        checkpointSpout45.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout40.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout60 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout60.activate();
        checkpointSpout60.activate();
        java.lang.Object obj63 = null;
        checkpointSpout60.fail(obj63);
        checkpointSpout60.close();
        checkpointSpout60.activate();
        checkpointSpout60.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout60.getComponentConfiguration();
        checkpointSpout54.fail((java.lang.Object) strMap68);
        checkpointSpout54.close();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout54);
        checkpointSpout32.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout40.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap68);
    }

    @Test
    public void test1024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1024");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer2 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1025");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.fail((java.lang.Object) (-1L));
        checkpointSpout28.fail((java.lang.Object) (-1.0f));
        checkpointSpout20.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout28.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1026");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) 'a');
        checkpointSpout19.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout19.activate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test1027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1027");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        java.lang.Object obj5 = null;
        checkpointSpout2.fail(obj5);
        checkpointSpout2.close();
        checkpointSpout2.close();
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.deactivate();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        java.lang.Class<?> wildcardClass32 = checkpointSpout25.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout2.fail((java.lang.Object) checkpointSpout11);
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout35.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout35.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout11.close();
        checkpointSpout11.fail((java.lang.Object) (short) 0);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass32);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap59);
    }

    @Test
    public void test1028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1028");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout25.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout16.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test1029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1029");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.activate();
    }

    @Test
    public void test1030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1030");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.activate();
        checkpointSpout41.activate();
        java.lang.Object obj44 = null;
        checkpointSpout41.fail(obj44);
        checkpointSpout41.deactivate();
        checkpointSpout34.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout41.deactivate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout41);
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        checkpointSpout7.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test1031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1031");
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
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout14.fail((java.lang.Object) strMap28);
        checkpointSpout14.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout14.activate();
        java.lang.Object obj33 = null;
        checkpointSpout14.fail(obj33);
        checkpointSpout14.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer36 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout14.declareOutputFields(outputFieldsDeclarer36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test1032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1032");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
    }

    @Test
    public void test1033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1033");
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
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        checkpointSpout17.fail((java.lang.Object) 10.0d);
        checkpointSpout17.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout26.activate();
        checkpointSpout26.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.fail((java.lang.Object) "action");
        checkpointSpout31.activate();
        checkpointSpout31.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout31.getComponentConfiguration();
        checkpointSpout31.activate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout31);
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.activate();
        checkpointSpout45.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout39.fail((java.lang.Object) strMap53);
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.fail((java.lang.Object) "action");
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        checkpointSpout55.fail((java.lang.Object) (short) -1);
        checkpointSpout55.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout64 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout64.activate();
        checkpointSpout64.activate();
        java.lang.Object obj67 = null;
        checkpointSpout64.fail(obj67);
        checkpointSpout64.close();
        checkpointSpout64.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout71 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout71.activate();
        checkpointSpout71.activate();
        java.lang.Object obj74 = null;
        checkpointSpout71.fail(obj74);
        checkpointSpout71.deactivate();
        checkpointSpout64.fail((java.lang.Object) checkpointSpout71);
        checkpointSpout64.activate();
        checkpointSpout55.fail((java.lang.Object) checkpointSpout64);
        checkpointSpout39.fail((java.lang.Object) checkpointSpout55);
        checkpointSpout31.fail((java.lang.Object) checkpointSpout55);
        checkpointSpout17.fail((java.lang.Object) checkpointSpout31);
        checkpointSpout31.close();
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test1034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1034");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
    }

    @Test
    public void test1035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1035");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout36.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.close();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout36);
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = null;
        org.apache.storm.task.TopologyContext topologyContext56 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector57 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout36.open(strMap55, topologyContext56, spoutOutputCollector57);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap52);
    }

    @Test
    public void test1036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1036");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.fail((java.lang.Object) "action");
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1037");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1038");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.activate();
        checkpointSpout41.activate();
        java.lang.Object obj44 = null;
        checkpointSpout41.fail(obj44);
        checkpointSpout41.deactivate();
        checkpointSpout34.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout41.deactivate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout41);
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout41.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = null;
        org.apache.storm.task.TopologyContext topologyContext52 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector53 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout41.open(strMap51, topologyContext52, spoutOutputCollector53);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test1039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1039");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap23);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1040");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        checkpointSpout33.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getState()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1041");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.fail((java.lang.Object) "action");
        checkpointSpout38.activate();
        checkpointSpout38.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout38.getComponentConfiguration();
        checkpointSpout38.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer51 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer51);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap43);
    }

    @Test
    public void test1042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1042");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test1043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1043");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout13.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        checkpointSpout21.fail((java.lang.Object) 'a');
        checkpointSpout21.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout21.getComponentConfiguration();
        checkpointSpout21.deactivate();
        checkpointSpout21.deactivate();
        checkpointSpout21.fail((java.lang.Object) (-1L));
        checkpointSpout21.fail((java.lang.Object) (-1.0f));
        checkpointSpout13.fail((java.lang.Object) checkpointSpout21);
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = checkpointSpout13.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNull(strMap40);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test1044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1044");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1045");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.activate();
        checkpointSpout24.fail((java.lang.Object) 'a');
        checkpointSpout24.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout24.getComponentConfiguration();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        checkpointSpout24.fail((java.lang.Object) (-1L));
        checkpointSpout24.fail((java.lang.Object) (-1.0f));
        checkpointSpout16.fail((java.lang.Object) checkpointSpout24);
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.deactivate();
        checkpointSpout16.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test1046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1046");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.activate();
        checkpointSpout41.activate();
        java.lang.Object obj44 = null;
        checkpointSpout41.fail(obj44);
        checkpointSpout41.deactivate();
        checkpointSpout34.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout41.deactivate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout7.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test1047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1047");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = null;
        org.apache.storm.task.TopologyContext topologyContext8 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector9 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap7, topologyContext8, spoutOutputCollector9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test1048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1048");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout13.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        checkpointSpout21.fail((java.lang.Object) 'a');
        checkpointSpout21.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout21.getComponentConfiguration();
        checkpointSpout21.deactivate();
        checkpointSpout21.deactivate();
        checkpointSpout21.fail((java.lang.Object) (-1L));
        checkpointSpout21.fail((java.lang.Object) (-1.0f));
        checkpointSpout13.fail((java.lang.Object) checkpointSpout21);
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = checkpointSpout13.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.close();
        checkpointSpout40.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.activate();
        checkpointSpout47.activate();
        java.lang.Object obj50 = null;
        checkpointSpout47.fail(obj50);
        checkpointSpout47.deactivate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout47.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout47);
        java.util.Map<java.lang.String, java.lang.Object> strMap56 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap56);
        org.apache.storm.spout.CheckpointSpout checkpointSpout58 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout58.activate();
        checkpointSpout58.activate();
        java.lang.Object obj61 = null;
        checkpointSpout58.fail(obj61);
        checkpointSpout58.close();
        checkpointSpout58.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout65 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout65.activate();
        checkpointSpout65.activate();
        java.lang.Object obj68 = null;
        checkpointSpout65.fail(obj68);
        checkpointSpout65.deactivate();
        checkpointSpout58.fail((java.lang.Object) checkpointSpout65);
        java.util.Map<java.lang.String, java.lang.Object> strMap72 = checkpointSpout58.getComponentConfiguration();
        checkpointSpout58.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout74 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout74.fail((java.lang.Object) "action");
        checkpointSpout74.activate();
        checkpointSpout74.activate();
        checkpointSpout74.deactivate();
        checkpointSpout74.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout81 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout81.fail((java.lang.Object) "action");
        checkpointSpout81.activate();
        checkpointSpout81.activate();
        checkpointSpout81.activate();
        checkpointSpout81.deactivate();
        checkpointSpout81.activate();
        checkpointSpout81.deactivate();
        checkpointSpout81.close();
        checkpointSpout74.fail((java.lang.Object) checkpointSpout81);
        checkpointSpout58.fail((java.lang.Object) checkpointSpout81);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout58);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNull(strMap56);
        org.junit.Assert.assertNull(strMap72);
    }

    @Test
    public void test1049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1049");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1050");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap7);
    }

    @Test
    public void test1051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1051");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        checkpointSpout0.close();
    }

    @Test
    public void test1052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1052");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1053");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout26.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout26.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout48 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout48.fail((java.lang.Object) "action");
        checkpointSpout48.activate();
        checkpointSpout48.close();
        java.lang.Class<?> wildcardClass53 = checkpointSpout48.getClass();
        checkpointSpout26.fail((java.lang.Object) wildcardClass53);
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        checkpointSpout55.close();
        checkpointSpout55.deactivate();
        checkpointSpout55.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap64 = checkpointSpout55.getComponentConfiguration();
        checkpointSpout55.close();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout55);
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass53);
        org.junit.Assert.assertNull(strMap64);
    }

    @Test
    public void test1054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1054");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.fail((java.lang.Object) (-1L));
        checkpointSpout28.fail((java.lang.Object) (-1.0f));
        checkpointSpout20.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = null;
        org.apache.storm.task.TopologyContext topologyContext50 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector51 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout28.open(strMap49, topologyContext50, spoutOutputCollector51);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1055");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.fail((java.lang.Object) "action");
        checkpointSpout38.activate();
        checkpointSpout38.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.fail((java.lang.Object) "action");
        checkpointSpout43.activate();
        checkpointSpout43.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout43.getComponentConfiguration();
        checkpointSpout43.activate();
        checkpointSpout38.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout38.deactivate();
        checkpointSpout38.deactivate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap48);
    }

    @Test
    public void test1056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1056");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        checkpointSpout3.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = null;
        org.apache.storm.task.TopologyContext topologyContext10 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout3.open(strMap9, topologyContext10, spoutOutputCollector11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test1057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1057");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.fail((java.lang.Object) "action");
        checkpointSpout28.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.deactivate();
        checkpointSpout32.fail((java.lang.Object) checkpointSpout39);
        checkpointSpout28.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout22.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout32);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer49 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout32.declareOutputFields(outputFieldsDeclarer49);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1058");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout42.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        java.lang.Object obj53 = null;
        checkpointSpout50.fail(obj53);
        checkpointSpout50.close();
        checkpointSpout50.activate();
        checkpointSpout50.fail((java.lang.Object) 'a');
        checkpointSpout50.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap60 = checkpointSpout50.getComponentConfiguration();
        checkpointSpout50.deactivate();
        checkpointSpout50.deactivate();
        checkpointSpout50.fail((java.lang.Object) (-1L));
        checkpointSpout50.fail((java.lang.Object) (-1.0f));
        checkpointSpout42.fail((java.lang.Object) checkpointSpout50);
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout42.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout69 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout69.activate();
        checkpointSpout69.activate();
        java.lang.Object obj72 = null;
        checkpointSpout69.fail(obj72);
        checkpointSpout69.close();
        checkpointSpout69.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout76 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout76.activate();
        checkpointSpout76.activate();
        java.lang.Object obj79 = null;
        checkpointSpout76.fail(obj79);
        checkpointSpout76.deactivate();
        checkpointSpout69.fail((java.lang.Object) checkpointSpout76);
        checkpointSpout76.deactivate();
        checkpointSpout42.fail((java.lang.Object) checkpointSpout76);
        java.util.Map<java.lang.String, java.lang.Object> strMap85 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout42);
        java.lang.Object obj88 = null;
        checkpointSpout42.fail(obj88);
        checkpointSpout42.activate();
        checkpointSpout42.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap60);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNull(strMap85);
    }

    @Test
    public void test1059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1059");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        checkpointSpout8.fail((java.lang.Object) 'a');
        checkpointSpout8.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
    }

    @Test
    public void test1060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1060");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        checkpointSpout23.activate();
        checkpointSpout23.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout23.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap32 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout23);
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = null;
        org.apache.storm.task.TopologyContext topologyContext36 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector37 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout23.open(strMap35, topologyContext36, spoutOutputCollector37);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap32);
    }

    @Test
    public void test1061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1061");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.fail((java.lang.Object) "action");
        checkpointSpout7.activate();
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.fail((java.lang.Object) "action");
        checkpointSpout12.activate();
        checkpointSpout12.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout12.getComponentConfiguration();
        checkpointSpout12.activate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout12);
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout20.fail((java.lang.Object) strMap34);
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.fail((java.lang.Object) "action");
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        checkpointSpout36.fail((java.lang.Object) (short) -1);
        checkpointSpout36.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.deactivate();
        checkpointSpout45.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout45.activate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout20.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout12.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout36);
        java.lang.Class<?> wildcardClass64 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass64);
    }

    @Test
    public void test1062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1062");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        checkpointSpout13.deactivate();
        checkpointSpout13.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        java.lang.Class<?> wildcardClass27 = checkpointSpout20.getClass();
        checkpointSpout13.fail((java.lang.Object) wildcardClass27);
        checkpointSpout11.fail((java.lang.Object) wildcardClass27);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap32 = checkpointSpout11.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
        org.junit.Assert.assertNull(strMap32);
    }

    @Test
    public void test1063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1063");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.fail((java.lang.Object) (short) -1);
        checkpointSpout10.close();
        checkpointSpout10.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout10.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1064");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.close();
        checkpointSpout15.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        checkpointSpout23.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.deactivate();
        checkpointSpout23.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout15.fail((java.lang.Object) strMap38);
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap13);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test1065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1065");
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
        checkpointSpout9.activate();
        checkpointSpout9.deactivate();
        checkpointSpout9.activate();
        checkpointSpout9.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout22.activate();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        checkpointSpout37.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout31.fail((java.lang.Object) strMap45);
        checkpointSpout31.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout31);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass52 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNotNull(wildcardClass52);
    }

    @Test
    public void test1066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1066");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout18.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout18.activate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1067");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.deactivate();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout8.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout12.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        checkpointSpout29.fail((java.lang.Object) 'a');
        checkpointSpout29.deactivate();
        checkpointSpout29.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout29.activate();
        checkpointSpout29.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout12.ack((java.lang.Object) checkpointSpout29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test1068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1068");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout48 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout48.activate();
        checkpointSpout48.activate();
        java.lang.Object obj51 = null;
        checkpointSpout48.fail(obj51);
        checkpointSpout48.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        checkpointSpout54.close();
        checkpointSpout48.fail((java.lang.Object) checkpointSpout54);
        checkpointSpout54.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout35.ack((java.lang.Object) checkpointSpout54);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1069");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout26.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNull(strMap48);
    }

    @Test
    public void test1070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1070");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
    }

    @Test
    public void test1071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1071");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout25.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test1072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1072");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1073");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1074");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.activate();
        checkpointSpout5.activate();
        java.lang.Object obj8 = null;
        checkpointSpout5.fail(obj8);
        checkpointSpout5.close();
        checkpointSpout5.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.deactivate();
        checkpointSpout5.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout5.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout5.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        checkpointSpout5.close();
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1075");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        checkpointSpout13.deactivate();
        checkpointSpout13.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        java.lang.Class<?> wildcardClass27 = checkpointSpout20.getClass();
        checkpointSpout13.fail((java.lang.Object) wildcardClass27);
        checkpointSpout11.fail((java.lang.Object) wildcardClass27);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout31);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test1076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1076");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout4.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout4.activate();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test1077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1077");
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
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout14.fail((java.lang.Object) strMap28);
        checkpointSpout14.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test1078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1078");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = null;
        org.apache.storm.task.TopologyContext topologyContext17 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap16, topologyContext17, spoutOutputCollector18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1079");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.deactivate();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout11.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        checkpointSpout30.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.deactivate();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout37.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout37);
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout37.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout37);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap29);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap47);
    }

    @Test
    public void test1080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1080");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.close();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout54);
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test1081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1081");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test1082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1082");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1083");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.fail((java.lang.Object) "action");
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout4.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout4.deactivate();
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        java.lang.Class<?> wildcardClass28 = checkpointSpout4.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test1084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1084");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.close();
        checkpointSpout37.deactivate();
        checkpointSpout37.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout37.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) checkpointSpout37);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1085");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        java.lang.Object obj5 = null;
        checkpointSpout2.fail(obj5);
        checkpointSpout2.close();
        checkpointSpout2.close();
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.deactivate();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        java.lang.Class<?> wildcardClass32 = checkpointSpout25.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout2.fail((java.lang.Object) checkpointSpout11);
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout35.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout35.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout11.close();
        checkpointSpout11.fail((java.lang.Object) (short) 0);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        java.lang.Class<?> wildcardClass59 = checkpointSpout11.getClass();
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass32);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNotNull(wildcardClass59);
    }

    @Test
    public void test1086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1086");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout25.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout16.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.fail((java.lang.Object) "action");
        checkpointSpout47.activate();
        checkpointSpout47.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        checkpointSpout54.close();
        checkpointSpout54.deactivate();
        checkpointSpout54.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout63 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout63.activate();
        checkpointSpout63.activate();
        java.lang.Object obj66 = null;
        checkpointSpout63.fail(obj66);
        checkpointSpout63.close();
        checkpointSpout63.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.activate();
        checkpointSpout70.activate();
        java.lang.Object obj73 = null;
        checkpointSpout70.fail(obj73);
        checkpointSpout70.deactivate();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout70);
        org.apache.storm.spout.CheckpointSpout checkpointSpout77 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout77.fail((java.lang.Object) "action");
        checkpointSpout77.activate();
        checkpointSpout77.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap82 = checkpointSpout77.getComponentConfiguration();
        checkpointSpout77.activate();
        java.lang.Class<?> wildcardClass84 = checkpointSpout77.getClass();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout77);
        checkpointSpout54.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout47.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout44.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout44.close();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout44);
        java.util.Map<java.lang.String, java.lang.Object> strMap91 = checkpointSpout44.getComponentConfiguration();
        checkpointSpout44.activate();
        checkpointSpout44.close();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap82);
        org.junit.Assert.assertNotNull(wildcardClass84);
        org.junit.Assert.assertNull(strMap91);
    }

    @Test
    public void test1087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1087");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout10.fail((java.lang.Object) strMap24);
        checkpointSpout10.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.deactivate();
        checkpointSpout10.deactivate();
        checkpointSpout10.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap24);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1088");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test1089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1089");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.activate();
        checkpointSpout41.activate();
        java.lang.Object obj44 = null;
        checkpointSpout41.fail(obj44);
        checkpointSpout41.deactivate();
        checkpointSpout34.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout41.deactivate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout41);
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout41.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout41.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
    }

    @Test
    public void test1090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1090");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = null;
        org.apache.storm.task.TopologyContext topologyContext11 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector12 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap10, topologyContext11, spoutOutputCollector12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1091");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout4.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = null;
        org.apache.storm.task.TopologyContext topologyContext25 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.open(strMap24, topologyContext25, spoutOutputCollector26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test1092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1092");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test1093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1093");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 0.0f);
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.close();
        checkpointSpout17.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.deactivate();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout24);
        checkpointSpout13.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout13.deactivate();
        checkpointSpout13.deactivate();
        checkpointSpout13.close();
        checkpointSpout13.close();
        checkpointSpout13.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.close();
    }

    @Test
    public void test1094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1094");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1095");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1096");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.deactivate();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout8.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout12.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout12.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout12.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test1097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1097");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout4.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        java.lang.Class<?> wildcardClass24 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test1098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1098");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout18.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout30);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout30.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getState()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test1099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1099");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = null;
        org.apache.storm.task.TopologyContext topologyContext13 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap12, topologyContext13, spoutOutputCollector14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1100");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = null;
        org.apache.storm.task.TopologyContext topologyContext38 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector39 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap37, topologyContext38, spoutOutputCollector39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test1101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1101");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1102");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap12);
    }

    @Test
    public void test1103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1103");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.close();
        checkpointSpout15.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        checkpointSpout23.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.deactivate();
        checkpointSpout23.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout15.fail((java.lang.Object) strMap38);
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.activate();
        org.junit.Assert.assertNull(strMap13);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test1104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1104");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout17.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNotNull(wildcardClass14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1105");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout13.deactivate();
        checkpointSpout13.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap31);
    }

    @Test
    public void test1106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1106");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1107");
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
        checkpointSpout9.activate();
        checkpointSpout9.deactivate();
        checkpointSpout9.activate();
        checkpointSpout9.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout22.activate();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        checkpointSpout37.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout31.fail((java.lang.Object) strMap45);
        checkpointSpout31.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout31);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout17.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap45);
    }

    @Test
    public void test1108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1108");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap23);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        checkpointSpout25.fail((java.lang.Object) 'a');
        checkpointSpout25.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.close();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout25.close();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test1109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1109");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = null;
        org.apache.storm.task.TopologyContext topologyContext10 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap9, topologyContext10, spoutOutputCollector11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1110");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test1111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1111");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = null;
        org.apache.storm.task.TopologyContext topologyContext35 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector36 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap34, topologyContext35, spoutOutputCollector36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test1112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1112");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = null;
        org.apache.storm.task.TopologyContext topologyContext31 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector32 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap30, topologyContext31, spoutOutputCollector32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap24);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1113");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1114");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1115");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.fail((java.lang.Object) (-1));
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.close();
        checkpointSpout10.deactivate();
        checkpointSpout10.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.fail((java.lang.Object) "action");
        checkpointSpout18.activate();
        checkpointSpout18.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.fail((java.lang.Object) "action");
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        checkpointSpout27.activate();
        checkpointSpout27.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.activate();
        checkpointSpout40.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout40.getComponentConfiguration();
        checkpointSpout40.activate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout35.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout49 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout49.activate();
        checkpointSpout49.activate();
        java.lang.Object obj52 = null;
        checkpointSpout49.fail(obj52);
        checkpointSpout49.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        checkpointSpout55.activate();
        checkpointSpout55.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap63 = checkpointSpout55.getComponentConfiguration();
        checkpointSpout49.fail((java.lang.Object) strMap63);
        checkpointSpout49.close();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout49);
        checkpointSpout27.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout18.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout18.deactivate();
        checkpointSpout18.fail((java.lang.Object) false);
        java.util.Map<java.lang.String, java.lang.Object> strMap72 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap72);
        org.apache.storm.spout.CheckpointSpout checkpointSpout74 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout74.fail((java.lang.Object) "action");
        checkpointSpout74.activate();
        checkpointSpout74.deactivate();
        checkpointSpout74.fail((java.lang.Object) ' ');
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) ' ');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap63);
        org.junit.Assert.assertNull(strMap72);
    }

    @Test
    public void test1116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1116");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test1117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1117");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test1118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1118");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 0.0f);
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        checkpointSpout14.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.deactivate();
        checkpointSpout14.fail((java.lang.Object) checkpointSpout21);
        checkpointSpout21.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout21.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout21);
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout21.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        checkpointSpout32.close();
        checkpointSpout32.deactivate();
        checkpointSpout32.fail((java.lang.Object) (byte) -1);
        checkpointSpout32.close();
        checkpointSpout32.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout21.ack((java.lang.Object) checkpointSpout32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap29);
        org.junit.Assert.assertNull(strMap31);
    }

    @Test
    public void test1119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1119");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test1120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1120");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        java.lang.Object obj6 = null;
        checkpointSpout3.fail(obj6);
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.fail((java.lang.Object) 'a');
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout3.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1121");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test1122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1122");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.close();
        checkpointSpout8.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout8.fail((java.lang.Object) strMap31);
        checkpointSpout8.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout8.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.close();
        checkpointSpout37.deactivate();
        checkpointSpout37.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        java.lang.Object obj49 = null;
        checkpointSpout46.fail(obj49);
        checkpointSpout46.close();
        checkpointSpout46.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        java.lang.Object obj56 = null;
        checkpointSpout53.fail(obj56);
        checkpointSpout53.deactivate();
        checkpointSpout46.fail((java.lang.Object) checkpointSpout53);
        org.apache.storm.spout.CheckpointSpout checkpointSpout60 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout60.fail((java.lang.Object) "action");
        checkpointSpout60.activate();
        checkpointSpout60.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap65 = checkpointSpout60.getComponentConfiguration();
        checkpointSpout60.activate();
        java.lang.Class<?> wildcardClass67 = checkpointSpout60.getClass();
        checkpointSpout46.fail((java.lang.Object) checkpointSpout60);
        checkpointSpout37.fail((java.lang.Object) checkpointSpout46);
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.activate();
        checkpointSpout70.activate();
        java.lang.Object obj73 = null;
        checkpointSpout70.fail(obj73);
        checkpointSpout70.close();
        checkpointSpout70.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout77 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout77.activate();
        checkpointSpout77.activate();
        java.lang.Object obj80 = null;
        checkpointSpout77.fail(obj80);
        checkpointSpout77.deactivate();
        checkpointSpout70.fail((java.lang.Object) checkpointSpout77);
        checkpointSpout70.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap85 = checkpointSpout70.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap86 = checkpointSpout70.getComponentConfiguration();
        checkpointSpout70.close();
        checkpointSpout70.close();
        checkpointSpout46.fail((java.lang.Object) checkpointSpout70);
        checkpointSpout70.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout70);
        java.lang.Class<?> wildcardClass92 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap65);
        org.junit.Assert.assertNotNull(wildcardClass67);
        org.junit.Assert.assertNull(strMap85);
        org.junit.Assert.assertNull(strMap86);
        org.junit.Assert.assertNotNull(wildcardClass92);
    }

    @Test
    public void test1123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1123");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap13);
    }

    @Test
    public void test1124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1124");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        checkpointSpout18.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1125");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = null;
        org.apache.storm.task.TopologyContext topologyContext25 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap24, topologyContext25, spoutOutputCollector26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1126");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 0.0f);
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test1127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1127");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.fail((java.lang.Object) "action");
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        checkpointSpout7.deactivate();
        checkpointSpout7.activate();
        checkpointSpout7.deactivate();
        checkpointSpout7.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = null;
        org.apache.storm.task.TopologyContext topologyContext19 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap18, topologyContext19, spoutOutputCollector20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1128");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.close();
        checkpointSpout7.deactivate();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout23);
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.fail((java.lang.Object) "action");
        checkpointSpout30.activate();
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.activate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout30.getClass();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout7.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        java.lang.Class<?> wildcardClass41 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNotNull(wildcardClass41);
    }

    @Test
    public void test1129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1129");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test1130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1130");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        checkpointSpout9.fail((java.lang.Object) 'a');
        checkpointSpout9.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.deactivate();
        checkpointSpout9.deactivate();
        checkpointSpout9.deactivate();
        checkpointSpout9.deactivate();
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        checkpointSpout30.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.deactivate();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout26.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout26.deactivate();
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout49 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout49.activate();
        checkpointSpout49.close();
        checkpointSpout49.deactivate();
        checkpointSpout49.deactivate();
        checkpointSpout49.deactivate();
        checkpointSpout49.close();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout49);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout26);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap47);
    }

    @Test
    public void test1131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1131");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.fail((java.lang.Object) (-1));
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test1132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1132");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.deactivate();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout8.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout12.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1133");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) 'a');
        checkpointSpout19.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout19.close();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test1134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1134");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout18.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout18.close();
        java.lang.Class<?> wildcardClass30 = checkpointSpout18.getClass();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test1135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1135");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1136");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout36.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.close();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout36);
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap58 = checkpointSpout36.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNull(strMap58);
    }

    @Test
    public void test1137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1137");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
    }

    @Test
    public void test1138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1138");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout35.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.deactivate();
        checkpointSpout46.activate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout46);
        java.lang.Object obj53 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout35.ack(obj53);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test1139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1139");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass6 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap4);
        org.junit.Assert.assertNotNull(wildcardClass6);
    }

    @Test
    public void test1140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1140");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout16.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass37);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.activate();
        checkpointSpout40.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.fail((java.lang.Object) "action");
        checkpointSpout45.activate();
        checkpointSpout45.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout45.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test1141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1141");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.fail((java.lang.Object) (short) -1);
        checkpointSpout20.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.deactivate();
        checkpointSpout29.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout29.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout29.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout29.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout29.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout29.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout29.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap47);
    }

    @Test
    public void test1142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1142");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test1143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1143");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) (short) 10);
        java.lang.Object obj11 = null;
        checkpointSpout0.fail(obj11);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = null;
        org.apache.storm.task.TopologyContext topologyContext15 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap14, topologyContext15, spoutOutputCollector16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1144");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test1145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1145");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1146");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout9.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = null;
        org.apache.storm.task.TopologyContext topologyContext46 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector47 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout9.open(strMap45, topologyContext46, spoutOutputCollector47);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test1147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1147");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.deactivate();
        checkpointSpout19.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout26.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        checkpointSpout34.fail((java.lang.Object) 'a');
        checkpointSpout34.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout34.getComponentConfiguration();
        checkpointSpout34.deactivate();
        checkpointSpout34.deactivate();
        checkpointSpout34.fail((java.lang.Object) (-1L));
        checkpointSpout34.fail((java.lang.Object) (-1.0f));
        checkpointSpout26.fail((java.lang.Object) checkpointSpout34);
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout26.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        java.lang.Object obj56 = null;
        checkpointSpout53.fail(obj56);
        checkpointSpout53.close();
        checkpointSpout53.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout60 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout60.activate();
        checkpointSpout60.activate();
        java.lang.Object obj63 = null;
        checkpointSpout60.fail(obj63);
        checkpointSpout60.deactivate();
        checkpointSpout53.fail((java.lang.Object) checkpointSpout60);
        checkpointSpout60.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout60);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout26);
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap71 = checkpointSpout70.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout72 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout72.fail((java.lang.Object) "action");
        checkpointSpout72.activate();
        checkpointSpout72.close();
        checkpointSpout72.deactivate();
        checkpointSpout72.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout79 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout79.fail((java.lang.Object) "action");
        checkpointSpout79.activate();
        checkpointSpout79.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap84 = checkpointSpout79.getComponentConfiguration();
        checkpointSpout79.activate();
        java.lang.Class<?> wildcardClass86 = checkpointSpout79.getClass();
        checkpointSpout72.fail((java.lang.Object) wildcardClass86);
        checkpointSpout70.fail((java.lang.Object) wildcardClass86);
        java.lang.Class<?> wildcardClass89 = checkpointSpout70.getClass();
        checkpointSpout26.fail((java.lang.Object) wildcardClass89);
        java.util.Map<java.lang.String, java.lang.Object> strMap91 = null;
        org.apache.storm.task.TopologyContext topologyContext92 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector93 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.open(strMap91, topologyContext92, spoutOutputCollector93);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap71);
        org.junit.Assert.assertNull(strMap84);
        org.junit.Assert.assertNotNull(wildcardClass86);
        org.junit.Assert.assertNotNull(wildcardClass89);
    }

    @Test
    public void test1148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1148");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.activate();
        checkpointSpout5.close();
        checkpointSpout5.deactivate();
        checkpointSpout5.deactivate();
        checkpointSpout5.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout5.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout13.activate();
        checkpointSpout13.close();
        checkpointSpout13.deactivate();
        checkpointSpout5.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout13.declareOutputFields(outputFieldsDeclarer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test1149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1149");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 0.0f);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
    }

    @Test
    public void test1150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1150");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = null;
        org.apache.storm.task.TopologyContext topologyContext25 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap24, topologyContext25, spoutOutputCollector26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test1151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1151");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout42.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        java.lang.Object obj53 = null;
        checkpointSpout50.fail(obj53);
        checkpointSpout50.close();
        checkpointSpout50.activate();
        checkpointSpout50.fail((java.lang.Object) 'a');
        checkpointSpout50.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap60 = checkpointSpout50.getComponentConfiguration();
        checkpointSpout50.deactivate();
        checkpointSpout50.deactivate();
        checkpointSpout50.fail((java.lang.Object) (-1L));
        checkpointSpout50.fail((java.lang.Object) (-1.0f));
        checkpointSpout42.fail((java.lang.Object) checkpointSpout50);
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout42.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout69 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout69.activate();
        checkpointSpout69.activate();
        java.lang.Object obj72 = null;
        checkpointSpout69.fail(obj72);
        checkpointSpout69.close();
        checkpointSpout69.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout76 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout76.activate();
        checkpointSpout76.activate();
        java.lang.Object obj79 = null;
        checkpointSpout76.fail(obj79);
        checkpointSpout76.deactivate();
        checkpointSpout69.fail((java.lang.Object) checkpointSpout76);
        checkpointSpout76.deactivate();
        checkpointSpout42.fail((java.lang.Object) checkpointSpout76);
        java.util.Map<java.lang.String, java.lang.Object> strMap85 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout42.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout42.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap60);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNull(strMap85);
    }

    @Test
    public void test1152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1152");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        java.lang.Object obj5 = null;
        checkpointSpout2.fail(obj5);
        checkpointSpout2.close();
        checkpointSpout2.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.deactivate();
        checkpointSpout2.fail((java.lang.Object) checkpointSpout9);
        checkpointSpout9.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.close();
        checkpointSpout17.activate();
        checkpointSpout17.fail((java.lang.Object) 'a');
        checkpointSpout17.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout17.getComponentConfiguration();
        checkpointSpout17.deactivate();
        checkpointSpout17.deactivate();
        checkpointSpout17.fail((java.lang.Object) (-1L));
        checkpointSpout17.fail((java.lang.Object) (-1.0f));
        checkpointSpout9.fail((java.lang.Object) checkpointSpout17);
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout17.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout17.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.fail((java.lang.Object) "action");
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        checkpointSpout37.deactivate();
        checkpointSpout37.activate();
        checkpointSpout37.close();
        checkpointSpout37.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout37);
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout37.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap47);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap47);
    }

    @Test
    public void test1153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1153");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.close();
        checkpointSpout12.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout27.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout12.fail((java.lang.Object) strMap35);
        checkpointSpout0.fail((java.lang.Object) strMap35);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer38 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer38);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test1154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1154");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout33);
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.close();
        checkpointSpout40.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.activate();
        checkpointSpout47.activate();
        java.lang.Object obj50 = null;
        checkpointSpout47.fail(obj50);
        checkpointSpout47.deactivate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout47.close();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout47);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer56 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout47.declareOutputFields(outputFieldsDeclarer56);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1155");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        checkpointSpout12.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout12.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap11);
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test1156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1156");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass26 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass26);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test1157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1157");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.fail((java.lang.Object) "action");
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = null;
        org.apache.storm.task.TopologyContext topologyContext19 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap18, topologyContext19, spoutOutputCollector20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1158");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout33.getComponentConfiguration();
        java.lang.Object obj54 = null;
        checkpointSpout33.fail(obj54);
        java.util.Map<java.lang.String, java.lang.Object> strMap56 = null;
        org.apache.storm.task.TopologyContext topologyContext57 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector58 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.open(strMap56, topologyContext57, spoutOutputCollector58);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test1159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1159");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.close();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        checkpointSpout30.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.deactivate();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout37.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.activate();
        checkpointSpout45.fail((java.lang.Object) 'a');
        checkpointSpout45.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.deactivate();
        checkpointSpout45.deactivate();
        checkpointSpout45.fail((java.lang.Object) (-1L));
        checkpointSpout45.fail((java.lang.Object) (-1.0f));
        checkpointSpout37.fail((java.lang.Object) checkpointSpout45);
        java.util.Map<java.lang.String, java.lang.Object> strMap63 = checkpointSpout37.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap64 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout37.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap66 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout24.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNull(strMap63);
        org.junit.Assert.assertNull(strMap64);
        org.junit.Assert.assertNull(strMap66);
    }

    @Test
    public void test1160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1160");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        checkpointSpout25.deactivate();
        checkpointSpout25.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout25.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout25.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
    }

    @Test
    public void test1161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1161");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.close();
    }

    @Test
    public void test1162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1162");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout36.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.close();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout36);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout36.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap52);
    }

    @Test
    public void test1163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1163");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1164");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        checkpointSpout9.fail((java.lang.Object) 1L);
        checkpointSpout9.deactivate();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test1165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1165");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.close();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        checkpointSpout30.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.deactivate();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout37.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.activate();
        checkpointSpout45.fail((java.lang.Object) 'a');
        checkpointSpout45.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.deactivate();
        checkpointSpout45.deactivate();
        checkpointSpout45.fail((java.lang.Object) (-1L));
        checkpointSpout45.fail((java.lang.Object) (-1.0f));
        checkpointSpout37.fail((java.lang.Object) checkpointSpout45);
        java.util.Map<java.lang.String, java.lang.Object> strMap63 = checkpointSpout37.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap64 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout37.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap66 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout24.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        checkpointSpout24.deactivate();
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNull(strMap63);
        org.junit.Assert.assertNull(strMap64);
        org.junit.Assert.assertNull(strMap66);
    }

    @Test
    public void test1166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1166");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass26 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass26);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNotNull(wildcardClass26);
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test1167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1167");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = null;
        org.apache.storm.task.TopologyContext topologyContext12 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap11, topologyContext12, spoutOutputCollector13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1168");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        java.lang.Class<?> wildcardClass22 = checkpointSpout8.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass22);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNotNull(wildcardClass22);
    }

    @Test
    public void test1169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1169");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        checkpointSpout9.fail((java.lang.Object) 'a');
        checkpointSpout9.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.deactivate();
        checkpointSpout9.deactivate();
        checkpointSpout9.fail((java.lang.Object) (-1L));
        checkpointSpout9.fail((java.lang.Object) (-1.0f));
        checkpointSpout9.close();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.fail((java.lang.Object) "action");
        checkpointSpout29.activate();
        checkpointSpout29.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.fail((java.lang.Object) "action");
        checkpointSpout34.activate();
        checkpointSpout34.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = checkpointSpout34.getComponentConfiguration();
        checkpointSpout34.activate();
        checkpointSpout29.fail((java.lang.Object) checkpointSpout34);
        checkpointSpout29.deactivate();
        java.lang.Class<?> wildcardClass43 = checkpointSpout29.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout9.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNotNull(wildcardClass43);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1170");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass10 = strMap9.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test1171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1171");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1172");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        checkpointSpout3.activate();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test1173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1173");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 0.0f);
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        checkpointSpout14.close();
        java.lang.Class<?> wildcardClass20 = checkpointSpout14.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test1174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1174");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout7.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test1175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1175");
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
        checkpointSpout5.deactivate();
        checkpointSpout5.deactivate();
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1176");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        java.lang.Object obj6 = null;
        checkpointSpout3.fail(obj6);
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.fail((java.lang.Object) 'a');
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1177");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass7 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1178");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.fail((java.lang.Object) (-1L));
        checkpointSpout28.fail((java.lang.Object) (-1.0f));
        checkpointSpout20.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.deactivate();
        checkpointSpout20.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout20.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout20.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
    }

    @Test
    public void test1179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1179");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout13.deactivate();
        checkpointSpout13.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = null;
        org.apache.storm.task.TopologyContext topologyContext32 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector33 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap31, topologyContext32, spoutOutputCollector33);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test1180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1180");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout13.deactivate();
        checkpointSpout13.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.close();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout36.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap45);
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap45);
    }

    @Test
    public void test1181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1181");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.close();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
        checkpointSpout23.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout23);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1182");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1183");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.close();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
        checkpointSpout23.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout23.deactivate();
        checkpointSpout23.activate();
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1184");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout10.activate();
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test1185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1185");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        checkpointSpout53.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout53);
        checkpointSpout53.activate();
        checkpointSpout53.deactivate();
        checkpointSpout53.close();
        java.lang.Class<?> wildcardClass61 = checkpointSpout53.getClass();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNotNull(wildcardClass61);
    }

    @Test
    public void test1186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1186");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1187");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1188");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout4.activate();
        checkpointSpout4.close();
    }

    @Test
    public void test1189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1189");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = null;
        org.apache.storm.task.TopologyContext topologyContext19 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap18, topologyContext19, spoutOutputCollector20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test1190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1190");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.activate();
        checkpointSpout5.close();
        checkpointSpout5.deactivate();
        checkpointSpout5.deactivate();
        checkpointSpout5.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout5);
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout5.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout5.declareOutputFields(outputFieldsDeclarer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
    }

    @Test
    public void test1191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1191");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.fail((java.lang.Object) "action");
        checkpointSpout12.activate();
        checkpointSpout12.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout12.getComponentConfiguration();
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.close();
        checkpointSpout19.deactivate();
        checkpointSpout19.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.deactivate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout35);
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.fail((java.lang.Object) "action");
        checkpointSpout42.activate();
        checkpointSpout42.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.activate();
        java.lang.Class<?> wildcardClass49 = checkpointSpout42.getClass();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout19.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout12.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test1192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1192");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass16);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1193");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap13);
    }

    @Test
    public void test1194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1194");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) 'a');
        checkpointSpout19.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.deactivate();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = null;
        org.apache.storm.task.TopologyContext topologyContext40 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector41 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap39, topologyContext40, spoutOutputCollector41);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test1195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1195");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass34 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.lang.Object obj39 = null;
        checkpointSpout0.fail(obj39);
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass34);
        org.junit.Assert.assertNull(strMap37);
    }

    @Test
    public void test1196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1196");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout43.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.activate();
        checkpointSpout51.activate();
        java.lang.Object obj54 = null;
        checkpointSpout51.fail(obj54);
        checkpointSpout51.close();
        checkpointSpout51.activate();
        checkpointSpout51.fail((java.lang.Object) 'a');
        checkpointSpout51.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap61 = checkpointSpout51.getComponentConfiguration();
        checkpointSpout51.deactivate();
        checkpointSpout51.deactivate();
        checkpointSpout51.fail((java.lang.Object) (-1L));
        checkpointSpout51.fail((java.lang.Object) (-1.0f));
        checkpointSpout43.fail((java.lang.Object) checkpointSpout51);
        java.util.Map<java.lang.String, java.lang.Object> strMap69 = checkpointSpout43.getComponentConfiguration();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout43);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap61);
        org.junit.Assert.assertNull(strMap69);
    }

    @Test
    public void test1197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1197");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout6.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout6.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) 'a');
        checkpointSpout16.close();
        checkpointSpout16.fail((java.lang.Object) 0.0f);
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.close();
        checkpointSpout16.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout16);
        java.lang.Object obj32 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout6.ack(obj32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test1198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1198");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap2);
    }

    @Test
    public void test1199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1199");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout33.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1200");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = null;
        org.apache.storm.task.TopologyContext topologyContext14 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector15 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap13, topologyContext14, spoutOutputCollector15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1201");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = null;
        org.apache.storm.task.TopologyContext topologyContext6 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap5, topologyContext6, spoutOutputCollector7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1202");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1203");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1204");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.deactivate();
        checkpointSpout45.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout52.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout60 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout60.activate();
        checkpointSpout60.activate();
        java.lang.Object obj63 = null;
        checkpointSpout60.fail(obj63);
        checkpointSpout60.close();
        checkpointSpout60.activate();
        checkpointSpout60.fail((java.lang.Object) 'a');
        checkpointSpout60.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout60.getComponentConfiguration();
        checkpointSpout60.deactivate();
        checkpointSpout60.deactivate();
        checkpointSpout60.fail((java.lang.Object) (-1L));
        checkpointSpout60.fail((java.lang.Object) (-1.0f));
        checkpointSpout52.fail((java.lang.Object) checkpointSpout60);
        checkpointSpout33.fail((java.lang.Object) checkpointSpout52);
        java.util.Map<java.lang.String, java.lang.Object> strMap79 = null;
        org.apache.storm.task.TopologyContext topologyContext80 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector81 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.open(strMap79, topologyContext80, spoutOutputCollector81);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap70);
    }

    @Test
    public void test1205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1205");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = null;
        org.apache.storm.task.TopologyContext topologyContext13 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap12, topologyContext13, spoutOutputCollector14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1206");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout16.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass37);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNotNull(wildcardClass37);
    }

    @Test
    public void test1207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1207");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.close();
        checkpointSpout19.deactivate();
        checkpointSpout19.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.deactivate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout35);
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.fail((java.lang.Object) "action");
        checkpointSpout42.activate();
        checkpointSpout42.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.activate();
        java.lang.Class<?> wildcardClass49 = checkpointSpout42.getClass();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout19.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout19.close();
        checkpointSpout19.deactivate();
        java.lang.Object obj54 = null;
        checkpointSpout19.fail(obj54);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout19.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass49);
        org.junit.Assert.assertNull(strMap57);
    }

    @Test
    public void test1208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1208");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test1209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1209");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass19 = checkpointSpout18.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout18.close();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1210");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1211");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1212");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = null;
        org.apache.storm.task.TopologyContext topologyContext5 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap4, topologyContext5, spoutOutputCollector6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
    }

    @Test
    public void test1213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1213");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.close();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout54);
        org.apache.storm.spout.CheckpointSpout checkpointSpout61 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout61.getComponentConfiguration();
        checkpointSpout61.close();
        checkpointSpout61.close();
        checkpointSpout54.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout61.activate();
        checkpointSpout61.deactivate();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap62);
    }

    @Test
    public void test1214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1214");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass34 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test1215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1215");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) (short) 10);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1216");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test1217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1217");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1218");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout7.fail((java.lang.Object) strMap21);
        checkpointSpout7.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        checkpointSpout7.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap24);
    }

    @Test
    public void test1219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1219");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass16 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1220");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout9.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        checkpointSpout31.activate();
        checkpointSpout31.fail((java.lang.Object) 'a');
        checkpointSpout31.deactivate();
        checkpointSpout31.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout31.getComponentConfiguration();
        checkpointSpout31.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout31.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap24);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test1221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1221");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        java.lang.Class<?> wildcardClass27 = checkpointSpout20.getClass();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1222");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout10.deactivate();
        checkpointSpout10.activate();
    }

    @Test
    public void test1223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1223");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test1224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1224");
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
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout22);
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test1225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1225");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.deactivate();
        checkpointSpout21.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        checkpointSpout36.fail((java.lang.Object) 'a');
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        checkpointSpout36.fail((java.lang.Object) (-1L));
        checkpointSpout36.fail((java.lang.Object) (-1.0f));
        checkpointSpout28.fail((java.lang.Object) checkpointSpout36);
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.fail((java.lang.Object) "action");
        checkpointSpout54.activate();
        checkpointSpout54.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout59 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout59.fail((java.lang.Object) "action");
        checkpointSpout59.activate();
        checkpointSpout59.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap64 = checkpointSpout59.getComponentConfiguration();
        checkpointSpout59.activate();
        checkpointSpout54.fail((java.lang.Object) checkpointSpout59);
        checkpointSpout54.deactivate();
        checkpointSpout54.activate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout54);
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.fail((java.lang.Object) "action");
        java.lang.Class<?> wildcardClass73 = checkpointSpout70.getClass();
        checkpointSpout54.fail((java.lang.Object) wildcardClass73);
        checkpointSpout0.fail((java.lang.Object) wildcardClass73);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap64);
        org.junit.Assert.assertNotNull(wildcardClass73);
    }

    @Test
    public void test1226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1226");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout14.getComponentConfiguration();
        checkpointSpout14.activate();
        java.lang.Class<?> wildcardClass21 = checkpointSpout14.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1227");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test1228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1228");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.activate();
        checkpointSpout15.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test1229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1229");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) (short) -1);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout25.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout16.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.fail((java.lang.Object) "action");
        checkpointSpout47.activate();
        checkpointSpout47.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        checkpointSpout54.close();
        checkpointSpout54.deactivate();
        checkpointSpout54.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout63 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout63.activate();
        checkpointSpout63.activate();
        java.lang.Object obj66 = null;
        checkpointSpout63.fail(obj66);
        checkpointSpout63.close();
        checkpointSpout63.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.activate();
        checkpointSpout70.activate();
        java.lang.Object obj73 = null;
        checkpointSpout70.fail(obj73);
        checkpointSpout70.deactivate();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout70);
        org.apache.storm.spout.CheckpointSpout checkpointSpout77 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout77.fail((java.lang.Object) "action");
        checkpointSpout77.activate();
        checkpointSpout77.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap82 = checkpointSpout77.getComponentConfiguration();
        checkpointSpout77.activate();
        java.lang.Class<?> wildcardClass84 = checkpointSpout77.getClass();
        checkpointSpout63.fail((java.lang.Object) checkpointSpout77);
        checkpointSpout54.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout47.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout44.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout44.close();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout44);
        java.util.Map<java.lang.String, java.lang.Object> strMap91 = checkpointSpout44.getComponentConfiguration();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap82);
        org.junit.Assert.assertNotNull(wildcardClass84);
        org.junit.Assert.assertNull(strMap91);
    }

    @Test
    public void test1230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1230");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
    }

    @Test
    public void test1231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1231");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap7);
    }

    @Test
    public void test1232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1232");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.close();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout54);
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.deactivate();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test1233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1233");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test1234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1234");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1235");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.activate();
        checkpointSpout51.activate();
        java.lang.Object obj54 = null;
        checkpointSpout51.fail(obj54);
        checkpointSpout51.close();
        checkpointSpout51.activate();
        checkpointSpout51.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout51.getComponentConfiguration();
        checkpointSpout45.fail((java.lang.Object) strMap59);
        checkpointSpout45.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        checkpointSpout45.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout45.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap59);
        org.junit.Assert.assertNull(strMap62);
    }

    @Test
    public void test1236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1236");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test1237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1237");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test1238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1238");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.close();
        checkpointSpout8.deactivate();
        checkpointSpout8.close();
        checkpointSpout8.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap18);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test1239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1239");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass19 = checkpointSpout18.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNotNull(wildcardClass19);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1240");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout14.getComponentConfiguration();
        checkpointSpout14.activate();
        java.lang.Class<?> wildcardClass21 = checkpointSpout14.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1241");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = null;
        org.apache.storm.task.TopologyContext topologyContext27 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector28 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap26, topologyContext27, spoutOutputCollector28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1242");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.close();
        checkpointSpout22.deactivate();
        checkpointSpout22.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.activate();
        checkpointSpout30.activate();
        java.lang.Object obj33 = null;
        checkpointSpout30.fail(obj33);
        checkpointSpout30.close();
        checkpointSpout30.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.deactivate();
        checkpointSpout30.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout22.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout22.deactivate();
        checkpointSpout22.activate();
        checkpointSpout22.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout48 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout48.getComponentConfiguration();
        checkpointSpout48.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout48.getComponentConfiguration();
        checkpointSpout48.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout48.getComponentConfiguration();
        checkpointSpout48.activate();
        checkpointSpout22.fail((java.lang.Object) checkpointSpout48);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test1243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1243");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = null;
        org.apache.storm.task.TopologyContext topologyContext25 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap24, topologyContext25, spoutOutputCollector26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test1244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1244");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test1245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1245");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test1246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1246");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        java.lang.Class<?> wildcardClass27 = checkpointSpout20.getClass();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1247");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        org.apache.storm.spout.CheckpointSpout checkpointSpout49 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout49.fail((java.lang.Object) "action");
        checkpointSpout49.activate();
        checkpointSpout49.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout49.getComponentConfiguration();
        checkpointSpout49.activate();
        java.lang.Class<?> wildcardClass56 = checkpointSpout49.getClass();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout49);
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout35.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap54);
        org.junit.Assert.assertNotNull(wildcardClass56);
        org.junit.Assert.assertNull(strMap59);
    }

    @Test
    public void test1248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1248");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        java.lang.Object obj14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1249");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout16.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass37);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.activate();
        checkpointSpout40.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.fail((java.lang.Object) "action");
        checkpointSpout45.activate();
        checkpointSpout45.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout45.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = null;
        org.apache.storm.task.TopologyContext topologyContext58 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector59 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap57, topologyContext58, spoutOutputCollector59);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test1250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1250");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1251");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.fail((java.lang.Object) (-1L));
        checkpointSpout28.fail((java.lang.Object) (-1.0f));
        checkpointSpout20.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.close();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.close();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1252");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout26.deactivate();
        checkpointSpout26.deactivate();
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test1253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1253");
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
        checkpointSpout9.activate();
        checkpointSpout9.deactivate();
        checkpointSpout9.activate();
        checkpointSpout9.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout22.activate();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout17.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        checkpointSpout37.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout31.fail((java.lang.Object) strMap45);
        checkpointSpout31.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout31);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap45);
    }

    @Test
    public void test1254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1254");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
    }

    @Test
    public void test1255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1255");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1256");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout35.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer46 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout35.declareOutputFields(outputFieldsDeclarer46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap45);
    }

    @Test
    public void test1257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1257");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass8 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test1258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1258");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass15 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1259");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.fail((java.lang.Object) "action");
        checkpointSpout38.activate();
        checkpointSpout38.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.fail((java.lang.Object) "action");
        checkpointSpout43.activate();
        checkpointSpout43.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout43.getComponentConfiguration();
        checkpointSpout43.activate();
        checkpointSpout38.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout16.deactivate();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap48);
    }

    @Test
    public void test1260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1260");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test1261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1261");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout10.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.activate();
        checkpointSpout10.close();
        checkpointSpout10.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.close();
        checkpointSpout31.deactivate();
        checkpointSpout31.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.ack((java.lang.Object) checkpointSpout31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1262");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.deactivate();
        checkpointSpout10.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) 'a');
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout19);
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        java.lang.Object obj41 = null;
        checkpointSpout38.fail(obj41);
        checkpointSpout38.close();
        checkpointSpout38.activate();
        checkpointSpout38.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout38.getComponentConfiguration();
        checkpointSpout32.fail((java.lang.Object) strMap46);
        checkpointSpout32.close();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout10.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout10.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.close();
        checkpointSpout52.activate();
        checkpointSpout52.deactivate();
        checkpointSpout52.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout52);
        org.junit.Assert.assertNull(strMap11);
        org.junit.Assert.assertNull(strMap13);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1263");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.deactivate();
        checkpointSpout21.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        checkpointSpout36.fail((java.lang.Object) 'a');
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        checkpointSpout36.fail((java.lang.Object) (-1L));
        checkpointSpout36.fail((java.lang.Object) (-1.0f));
        checkpointSpout28.fail((java.lang.Object) checkpointSpout36);
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout28.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        checkpointSpout55.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout62 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout62.activate();
        checkpointSpout62.activate();
        java.lang.Object obj65 = null;
        checkpointSpout62.fail(obj65);
        checkpointSpout62.deactivate();
        checkpointSpout55.fail((java.lang.Object) checkpointSpout62);
        checkpointSpout62.deactivate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout62);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout62);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap54);
    }

    @Test
    public void test1264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1264");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout4.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        java.lang.Class<?> wildcardClass24 = checkpointSpout4.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test1265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1265");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.close();
        checkpointSpout25.deactivate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        checkpointSpout32.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.fail((java.lang.Object) "action");
        checkpointSpout37.activate();
        checkpointSpout37.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout37.activate();
        checkpointSpout32.fail((java.lang.Object) checkpointSpout37);
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.activate();
        checkpointSpout51.activate();
        java.lang.Object obj54 = null;
        checkpointSpout51.fail(obj54);
        checkpointSpout51.close();
        checkpointSpout51.activate();
        checkpointSpout51.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout51.getComponentConfiguration();
        checkpointSpout45.fail((java.lang.Object) strMap59);
        org.apache.storm.spout.CheckpointSpout checkpointSpout61 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout61.fail((java.lang.Object) "action");
        checkpointSpout61.activate();
        checkpointSpout61.activate();
        checkpointSpout61.activate();
        checkpointSpout61.fail((java.lang.Object) (short) -1);
        checkpointSpout61.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout70 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout70.activate();
        checkpointSpout70.activate();
        java.lang.Object obj73 = null;
        checkpointSpout70.fail(obj73);
        checkpointSpout70.close();
        checkpointSpout70.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout77 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout77.activate();
        checkpointSpout77.activate();
        java.lang.Object obj80 = null;
        checkpointSpout77.fail(obj80);
        checkpointSpout77.deactivate();
        checkpointSpout70.fail((java.lang.Object) checkpointSpout77);
        checkpointSpout70.activate();
        checkpointSpout61.fail((java.lang.Object) checkpointSpout70);
        checkpointSpout45.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout37.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout25.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout25.activate();
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap59);
    }

    @Test
    public void test1266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1266");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1267");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout11.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass19 = checkpointSpout18.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = null;
        org.apache.storm.task.TopologyContext topologyContext25 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap24, topologyContext25, spoutOutputCollector26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNotNull(wildcardClass19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test1268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1268");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout6.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout6.deactivate();
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test1269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1269");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout42.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        java.lang.Object obj53 = null;
        checkpointSpout50.fail(obj53);
        checkpointSpout50.close();
        checkpointSpout50.activate();
        checkpointSpout50.fail((java.lang.Object) 'a');
        checkpointSpout50.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap60 = checkpointSpout50.getComponentConfiguration();
        checkpointSpout50.deactivate();
        checkpointSpout50.deactivate();
        checkpointSpout50.fail((java.lang.Object) (-1L));
        checkpointSpout50.fail((java.lang.Object) (-1.0f));
        checkpointSpout42.fail((java.lang.Object) checkpointSpout50);
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout42.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout69 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout69.activate();
        checkpointSpout69.activate();
        java.lang.Object obj72 = null;
        checkpointSpout69.fail(obj72);
        checkpointSpout69.close();
        checkpointSpout69.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout76 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout76.activate();
        checkpointSpout76.activate();
        java.lang.Object obj79 = null;
        checkpointSpout76.fail(obj79);
        checkpointSpout76.deactivate();
        checkpointSpout69.fail((java.lang.Object) checkpointSpout76);
        checkpointSpout76.deactivate();
        checkpointSpout42.fail((java.lang.Object) checkpointSpout76);
        java.util.Map<java.lang.String, java.lang.Object> strMap85 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout42.close();
        checkpointSpout42.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout42.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap60);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNull(strMap85);
    }

    @Test
    public void test1270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1270");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.fail((java.lang.Object) "action");
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout18.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        java.lang.Object obj41 = null;
        checkpointSpout38.fail(obj41);
        checkpointSpout38.close();
        checkpointSpout38.activate();
        checkpointSpout38.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout38.getComponentConfiguration();
        checkpointSpout32.fail((java.lang.Object) strMap46);
        checkpointSpout32.close();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test1271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1271");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        checkpointSpout26.fail((java.lang.Object) (byte) 1);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout13.ack((java.lang.Object) checkpointSpout26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test1272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1272");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.close();
        checkpointSpout8.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout8.fail((java.lang.Object) strMap31);
        checkpointSpout8.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout8.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout8.deactivate();
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1273");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.fail((java.lang.Object) "action");
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.deactivate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout16.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout16.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass37);
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass40 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNotNull(wildcardClass40);
    }

    @Test
    public void test1274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1274");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass16 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1275");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.deactivate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap41);
    }

    @Test
    public void test1276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1276");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.close();
        checkpointSpout24.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.deactivate();
        checkpointSpout32.fail((java.lang.Object) checkpointSpout39);
        checkpointSpout39.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout39.getComponentConfiguration();
        checkpointSpout24.fail((java.lang.Object) strMap47);
        java.lang.Class<?> wildcardClass49 = checkpointSpout24.getClass();
        checkpointSpout8.fail((java.lang.Object) wildcardClass49);
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = null;
        org.apache.storm.task.TopologyContext topologyContext52 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector53 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.open(strMap51, topologyContext52, spoutOutputCollector53);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test1277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1277");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout13.deactivate();
        checkpointSpout13.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        checkpointSpout29.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout29);
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.close();
        checkpointSpout38.deactivate();
        checkpointSpout38.deactivate();
        checkpointSpout38.deactivate();
        checkpointSpout38.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout38.activate();
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test1278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1278");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout10.close();
        checkpointSpout10.deactivate();
    }

    @Test
    public void test1279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1279");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1280");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.fail((java.lang.Object) "action");
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout4.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout4.deactivate();
        checkpointSpout4.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout4.getComponentConfiguration();
        checkpointSpout4.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.activate();
        checkpointSpout28.deactivate();
        checkpointSpout28.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test1281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1281");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer41 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.declareOutputFields(outputFieldsDeclarer41);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1282");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout23);
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.fail((java.lang.Object) "action");
        checkpointSpout30.activate();
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.activate();
        java.lang.Class<?> wildcardClass37 = checkpointSpout30.getClass();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout16.deactivate();
        java.lang.Class<?> wildcardClass40 = checkpointSpout16.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNotNull(wildcardClass40);
    }

    @Test
    public void test1283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1283");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.deactivate();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout19);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout26.activate();
        checkpointSpout26.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout26.activate();
        java.lang.Class<?> wildcardClass33 = checkpointSpout26.getClass();
        checkpointSpout12.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout12.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.deactivate();
        checkpointSpout37.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout44.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.close();
        checkpointSpout52.activate();
        checkpointSpout52.fail((java.lang.Object) 'a');
        checkpointSpout52.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout52.getComponentConfiguration();
        checkpointSpout52.deactivate();
        checkpointSpout52.deactivate();
        checkpointSpout52.fail((java.lang.Object) (-1L));
        checkpointSpout52.fail((java.lang.Object) (-1.0f));
        checkpointSpout44.fail((java.lang.Object) checkpointSpout52);
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout44.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap71 = checkpointSpout44.getComponentConfiguration();
        checkpointSpout44.deactivate();
        java.lang.Class<?> wildcardClass73 = checkpointSpout44.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout44);
        java.util.Map<java.lang.String, java.lang.Object> strMap75 = checkpointSpout44.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNotNull(wildcardClass33);
        org.junit.Assert.assertNull(strMap62);
        org.junit.Assert.assertNull(strMap70);
        org.junit.Assert.assertNull(strMap71);
        org.junit.Assert.assertNotNull(wildcardClass73);
        org.junit.Assert.assertNull(strMap75);
    }

    @Test
    public void test1284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1284");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout36.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.close();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout36);
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap55);
    }

    @Test
    public void test1285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1285");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass7 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1286");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout9.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        checkpointSpout25.deactivate();
        checkpointSpout25.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout25.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout25.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout25);
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        java.lang.Object obj53 = null;
        checkpointSpout50.fail(obj53);
        checkpointSpout50.close();
        checkpointSpout50.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout57 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout57.activate();
        checkpointSpout57.activate();
        java.lang.Object obj60 = null;
        checkpointSpout57.fail(obj60);
        checkpointSpout57.deactivate();
        checkpointSpout50.fail((java.lang.Object) checkpointSpout57);
        checkpointSpout57.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout65 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout65.activate();
        checkpointSpout65.activate();
        java.lang.Object obj68 = null;
        checkpointSpout65.fail(obj68);
        checkpointSpout65.close();
        checkpointSpout65.activate();
        checkpointSpout65.fail((java.lang.Object) 'a');
        checkpointSpout65.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap75 = checkpointSpout65.getComponentConfiguration();
        checkpointSpout65.deactivate();
        checkpointSpout65.deactivate();
        checkpointSpout65.fail((java.lang.Object) (-1L));
        checkpointSpout65.fail((java.lang.Object) (-1.0f));
        checkpointSpout57.fail((java.lang.Object) checkpointSpout65);
        java.util.Map<java.lang.String, java.lang.Object> strMap83 = checkpointSpout57.getComponentConfiguration();
        checkpointSpout57.deactivate();
        checkpointSpout57.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout57);
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass88 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap75);
        org.junit.Assert.assertNull(strMap83);
        org.junit.Assert.assertNotNull(wildcardClass88);
    }

    @Test
    public void test1287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1287");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        checkpointSpout34.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.activate();
        checkpointSpout41.activate();
        java.lang.Object obj44 = null;
        checkpointSpout41.fail(obj44);
        checkpointSpout41.deactivate();
        checkpointSpout34.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout41.deactivate();
        checkpointSpout7.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout7.fail((java.lang.Object) (byte) 100);
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        checkpointSpout52.deactivate();
        checkpointSpout52.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout52.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) checkpointSpout52);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap57);
    }

    @Test
    public void test1288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1288");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test1289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1289");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout7.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1290");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test1291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1291");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.deactivate();
        checkpointSpout21.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout21.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout21.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout21.getComponentConfiguration();
        checkpointSpout21.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = checkpointSpout21.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.close();
        checkpointSpout40.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.activate();
        checkpointSpout47.activate();
        java.lang.Object obj50 = null;
        checkpointSpout47.fail(obj50);
        checkpointSpout47.deactivate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout47.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) checkpointSpout47);
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.close();
        checkpointSpout47.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout47);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNull(strMap57);
    }

    @Test
    public void test1292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1292");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout10.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.deactivate();
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1293");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout17.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout17.getComponentConfiguration();
        checkpointSpout17.activate();
        java.lang.Class<?> wildcardClass25 = checkpointSpout17.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass25);
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.fail((java.lang.Object) "action");
        checkpointSpout28.fail((java.lang.Object) "action");
        checkpointSpout0.fail((java.lang.Object) "action");
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = null;
        org.apache.storm.task.TopologyContext topologyContext45 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector46 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap44, topologyContext45, spoutOutputCollector46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNotNull(wildcardClass25);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test1294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1294");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        checkpointSpout3.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout8.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        checkpointSpout24.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = null;
        org.apache.storm.task.TopologyContext topologyContext34 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout24.open(strMap33, topologyContext34, spoutOutputCollector35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap4);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test1295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1295");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.deactivate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout25.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout33.fail((java.lang.Object) 'a');
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.deactivate();
        checkpointSpout33.fail((java.lang.Object) (-1L));
        checkpointSpout33.fail((java.lang.Object) (-1.0f));
        checkpointSpout25.fail((java.lang.Object) checkpointSpout33);
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout25.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.activate();
        checkpointSpout52.activate();
        java.lang.Object obj55 = null;
        checkpointSpout52.fail(obj55);
        checkpointSpout52.close();
        checkpointSpout52.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout59 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout59.activate();
        checkpointSpout59.activate();
        java.lang.Object obj62 = null;
        checkpointSpout59.fail(obj62);
        checkpointSpout59.deactivate();
        checkpointSpout52.fail((java.lang.Object) checkpointSpout59);
        checkpointSpout59.deactivate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout59);
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout59.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap68);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap68);
    }

    @Test
    public void test1296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1296");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass26 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass26);
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        checkpointSpout29.fail((java.lang.Object) 'a');
        checkpointSpout29.deactivate();
        checkpointSpout29.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout29.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout29);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNotNull(wildcardClass26);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test1297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1297");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass20 = strMap19.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test1298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1298");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        java.lang.Object obj12 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1299");
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
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        checkpointSpout17.fail((java.lang.Object) 10.0d);
        checkpointSpout17.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.deactivate();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout26.close();
        checkpointSpout26.activate();
        checkpointSpout26.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.fail((java.lang.Object) "action");
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        checkpointSpout46.deactivate();
        java.lang.Class<?> wildcardClass52 = checkpointSpout46.getClass();
        checkpointSpout26.fail((java.lang.Object) wildcardClass52);
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout17.fail((java.lang.Object) strMap54);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNotNull(wildcardClass52);
        org.junit.Assert.assertNull(strMap54);
    }

    @Test
    public void test1300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1300");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        checkpointSpout9.fail((java.lang.Object) 'a');
        checkpointSpout9.close();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout22.fail((java.lang.Object) strMap36);
        checkpointSpout22.close();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test1301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1301");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.fail((java.lang.Object) 'a');
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout13);
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout13.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout13.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout28.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout28.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.fail((java.lang.Object) "action");
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        checkpointSpout38.fail((java.lang.Object) (short) -1);
        checkpointSpout38.close();
        checkpointSpout38.close();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout38);
        checkpointSpout28.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout13.ack((java.lang.Object) checkpointSpout28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap37);
    }

    @Test
    public void test1302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1302");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout6.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout6.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) 'a');
        checkpointSpout16.close();
        checkpointSpout16.fail((java.lang.Object) 0.0f);
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.close();
        checkpointSpout16.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.deactivate();
        checkpointSpout32.fail((java.lang.Object) checkpointSpout39);
        checkpointSpout39.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.activate();
        checkpointSpout47.activate();
        java.lang.Object obj50 = null;
        checkpointSpout47.fail(obj50);
        checkpointSpout47.close();
        checkpointSpout47.activate();
        checkpointSpout47.fail((java.lang.Object) 'a');
        checkpointSpout47.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.deactivate();
        checkpointSpout47.deactivate();
        checkpointSpout47.fail((java.lang.Object) (-1L));
        checkpointSpout47.fail((java.lang.Object) (-1.0f));
        checkpointSpout39.fail((java.lang.Object) checkpointSpout47);
        java.util.Map<java.lang.String, java.lang.Object> strMap65 = checkpointSpout39.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout66 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout66.activate();
        checkpointSpout66.activate();
        java.lang.Object obj69 = null;
        checkpointSpout66.fail(obj69);
        checkpointSpout66.close();
        checkpointSpout66.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout73 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout73.activate();
        checkpointSpout73.activate();
        java.lang.Object obj76 = null;
        checkpointSpout73.fail(obj76);
        checkpointSpout73.deactivate();
        checkpointSpout66.fail((java.lang.Object) checkpointSpout73);
        checkpointSpout73.deactivate();
        checkpointSpout39.fail((java.lang.Object) checkpointSpout73);
        java.util.Map<java.lang.String, java.lang.Object> strMap82 = checkpointSpout39.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.ack((java.lang.Object) strMap82);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNull(strMap57);
        org.junit.Assert.assertNull(strMap65);
        org.junit.Assert.assertNull(strMap82);
    }

    @Test
    public void test1303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1303");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        checkpointSpout10.fail((java.lang.Object) (short) -1);
        checkpointSpout10.close();
        checkpointSpout10.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        checkpointSpout22.close();
        checkpointSpout22.deactivate();
        checkpointSpout22.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.activate();
        checkpointSpout31.activate();
        java.lang.Object obj34 = null;
        checkpointSpout31.fail(obj34);
        checkpointSpout31.close();
        checkpointSpout31.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout38 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout38.activate();
        checkpointSpout38.activate();
        java.lang.Object obj41 = null;
        checkpointSpout38.fail(obj41);
        checkpointSpout38.deactivate();
        checkpointSpout31.fail((java.lang.Object) checkpointSpout38);
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.fail((java.lang.Object) "action");
        checkpointSpout45.activate();
        checkpointSpout45.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        java.lang.Class<?> wildcardClass52 = checkpointSpout45.getClass();
        checkpointSpout31.fail((java.lang.Object) checkpointSpout45);
        checkpointSpout22.fail((java.lang.Object) checkpointSpout31);
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        checkpointSpout55.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout62 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout62.activate();
        checkpointSpout62.activate();
        java.lang.Object obj65 = null;
        checkpointSpout62.fail(obj65);
        checkpointSpout62.deactivate();
        checkpointSpout55.fail((java.lang.Object) checkpointSpout62);
        checkpointSpout55.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout55.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap71 = checkpointSpout55.getComponentConfiguration();
        checkpointSpout55.close();
        checkpointSpout55.close();
        checkpointSpout31.fail((java.lang.Object) checkpointSpout55);
        checkpointSpout55.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout76 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout76.activate();
        checkpointSpout76.close();
        java.lang.Object obj79 = null;
        checkpointSpout76.fail(obj79);
        checkpointSpout76.activate();
        checkpointSpout55.fail((java.lang.Object) checkpointSpout76);
        org.apache.storm.spout.CheckpointSpout checkpointSpout83 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap84 = checkpointSpout83.getComponentConfiguration();
        checkpointSpout83.close();
        checkpointSpout83.close();
        checkpointSpout76.fail((java.lang.Object) checkpointSpout83);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout76);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNotNull(wildcardClass52);
        org.junit.Assert.assertNull(strMap70);
        org.junit.Assert.assertNull(strMap71);
        org.junit.Assert.assertNull(strMap84);
    }

    @Test
    public void test1304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1304");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.close();
        checkpointSpout33.close();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.close();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.activate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout54);
        org.apache.storm.spout.CheckpointSpout checkpointSpout61 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout61.getComponentConfiguration();
        checkpointSpout61.close();
        checkpointSpout61.close();
        checkpointSpout54.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout54.close();
        java.lang.Class<?> wildcardClass67 = checkpointSpout54.getClass();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap62);
        org.junit.Assert.assertNotNull(wildcardClass67);
    }

    @Test
    public void test1305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1305");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.close();
        checkpointSpout18.deactivate();
        checkpointSpout18.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.deactivate();
        checkpointSpout27.fail((java.lang.Object) checkpointSpout34);
        org.apache.storm.spout.CheckpointSpout checkpointSpout41 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout41.fail((java.lang.Object) "action");
        checkpointSpout41.activate();
        checkpointSpout41.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout41.getComponentConfiguration();
        checkpointSpout41.activate();
        java.lang.Class<?> wildcardClass48 = checkpointSpout41.getClass();
        checkpointSpout27.fail((java.lang.Object) checkpointSpout41);
        checkpointSpout18.fail((java.lang.Object) checkpointSpout27);
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.activate();
        checkpointSpout51.activate();
        java.lang.Object obj54 = null;
        checkpointSpout51.fail(obj54);
        checkpointSpout51.close();
        checkpointSpout51.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout58 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout58.activate();
        checkpointSpout58.activate();
        java.lang.Object obj61 = null;
        checkpointSpout58.fail(obj61);
        checkpointSpout58.deactivate();
        checkpointSpout51.fail((java.lang.Object) checkpointSpout58);
        checkpointSpout51.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap66 = checkpointSpout51.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap67 = checkpointSpout51.getComponentConfiguration();
        checkpointSpout51.close();
        checkpointSpout51.close();
        checkpointSpout27.fail((java.lang.Object) checkpointSpout51);
        org.apache.storm.spout.CheckpointSpout checkpointSpout71 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout71.activate();
        checkpointSpout71.activate();
        checkpointSpout71.deactivate();
        checkpointSpout51.fail((java.lang.Object) checkpointSpout71);
        checkpointSpout71.activate();
        checkpointSpout71.deactivate();
        checkpointSpout71.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout71);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout71.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getState()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNotNull(wildcardClass48);
        org.junit.Assert.assertNull(strMap66);
        org.junit.Assert.assertNull(strMap67);
    }

    @Test
    public void test1306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1306");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.close();
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        checkpointSpout29.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout23.fail((java.lang.Object) strMap37);
        checkpointSpout23.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
        checkpointSpout23.close();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout23);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer46 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout23.declareOutputFields(outputFieldsDeclarer46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test1307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1307");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test1308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1308");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.activate();
        checkpointSpout24.fail((java.lang.Object) 'a');
        checkpointSpout24.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout24.getComponentConfiguration();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        checkpointSpout24.fail((java.lang.Object) (-1L));
        checkpointSpout24.fail((java.lang.Object) (-1.0f));
        checkpointSpout24.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = null;
        org.apache.storm.task.TopologyContext topologyContext44 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector45 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap43, topologyContext44, spoutOutputCollector45);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1309");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        java.lang.Object obj5 = null;
        checkpointSpout2.fail(obj5);
        checkpointSpout2.close();
        checkpointSpout2.close();
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.deactivate();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout18);
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        java.lang.Class<?> wildcardClass32 = checkpointSpout25.getClass();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout2.fail((java.lang.Object) checkpointSpout11);
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.deactivate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout35.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout35.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout11.close();
        checkpointSpout11.fail((java.lang.Object) (short) 0);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout60 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout60.activate();
        checkpointSpout60.activate();
        java.lang.Object obj63 = null;
        checkpointSpout60.fail(obj63);
        checkpointSpout60.close();
        checkpointSpout60.activate();
        checkpointSpout60.fail((java.lang.Object) 'a');
        checkpointSpout60.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout60.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout71 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout71.fail((java.lang.Object) "action");
        checkpointSpout60.fail((java.lang.Object) "action");
        checkpointSpout60.deactivate();
        checkpointSpout60.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap77 = checkpointSpout60.getComponentConfiguration();
        java.lang.Class<?> wildcardClass78 = checkpointSpout60.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass78);
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass32);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNull(strMap70);
        org.junit.Assert.assertNull(strMap77);
        org.junit.Assert.assertNotNull(wildcardClass78);
    }

    @Test
    public void test1310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1310");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout35.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.deactivate();
        checkpointSpout46.activate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout46);
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout35.deactivate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test1311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1311");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.fail((java.lang.Object) "action");
        checkpointSpout14.activate();
        checkpointSpout14.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout14.getComponentConfiguration();
        checkpointSpout14.activate();
        java.lang.Class<?> wildcardClass21 = checkpointSpout14.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1312");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        checkpointSpout4.deactivate();
        checkpointSpout4.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = null;
        org.apache.storm.task.TopologyContext topologyContext22 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector23 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.open(strMap21, topologyContext22, spoutOutputCollector23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1313");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.close();
        checkpointSpout17.activate();
        checkpointSpout17.fail((java.lang.Object) 'a');
        checkpointSpout17.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout17.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.fail((java.lang.Object) "action");
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.deactivate();
        checkpointSpout17.close();
        checkpointSpout17.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test1314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1314");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap14);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        checkpointSpout23.close();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout17.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap32 = checkpointSpout17.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.deactivate();
        checkpointSpout37.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout33.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout33.deactivate();
        checkpointSpout33.deactivate();
        java.lang.Class<?> wildcardClass54 = checkpointSpout33.getClass();
        checkpointSpout17.fail((java.lang.Object) wildcardClass54);
        checkpointSpout17.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout17);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap32);
        org.junit.Assert.assertNotNull(wildcardClass54);
    }

    @Test
    public void test1315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1315");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test1316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1316");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1317");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1318");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.deactivate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap23);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.fail((java.lang.Object) "action");
        checkpointSpout30.activate();
        checkpointSpout30.deactivate();
        checkpointSpout30.fail((java.lang.Object) ' ');
        checkpointSpout30.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test1319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1319");
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
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test1320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1320");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass34 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout39 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout39.activate();
        checkpointSpout39.activate();
        java.lang.Object obj42 = null;
        checkpointSpout39.fail(obj42);
        checkpointSpout39.close();
        checkpointSpout39.close();
        checkpointSpout39.deactivate();
        checkpointSpout39.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout39.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout39.getComponentConfiguration();
        checkpointSpout39.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass34);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test1321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1321");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.deactivate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout18.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout18.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout18.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.deactivate();
        checkpointSpout37.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout37.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout37.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout37.getComponentConfiguration();
        checkpointSpout37.close();
        checkpointSpout37.activate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout18.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test1322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1322");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1323");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        checkpointSpout15.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.close();
        checkpointSpout24.deactivate();
        checkpointSpout24.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.deactivate();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout40);
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.fail((java.lang.Object) "action");
        checkpointSpout47.activate();
        checkpointSpout47.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap52 = checkpointSpout47.getComponentConfiguration();
        checkpointSpout47.activate();
        java.lang.Class<?> wildcardClass54 = checkpointSpout47.getClass();
        checkpointSpout33.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout24.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout24.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap58 = checkpointSpout24.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap58);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap52);
        org.junit.Assert.assertNotNull(wildcardClass54);
        org.junit.Assert.assertNull(strMap58);
    }

    @Test
    public void test1324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1324");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
    }

    @Test
    public void test1325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1325");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        checkpointSpout22.close();
        checkpointSpout22.close();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        checkpointSpout22.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout22);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1326");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        checkpointSpout20.activate();
        checkpointSpout20.close();
        checkpointSpout20.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.close();
        checkpointSpout29.activate();
        checkpointSpout29.fail((java.lang.Object) 'a');
        checkpointSpout29.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap39 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout29.deactivate();
        checkpointSpout29.deactivate();
        checkpointSpout29.deactivate();
        checkpointSpout29.deactivate();
        checkpointSpout29.activate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.fail((java.lang.Object) "action");
        checkpointSpout47.activate();
        checkpointSpout47.close();
        checkpointSpout47.deactivate();
        checkpointSpout47.fail((java.lang.Object) (byte) -1);
        checkpointSpout47.deactivate();
        checkpointSpout47.close();
        java.lang.Class<?> wildcardClass57 = checkpointSpout47.getClass();
        checkpointSpout20.fail((java.lang.Object) wildcardClass57);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNotNull(wildcardClass57);
    }

    @Test
    public void test1327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1327");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout20.getComponentConfiguration();
        checkpointSpout20.activate();
        java.lang.Class<?> wildcardClass27 = checkpointSpout20.getClass();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1328");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout7.close();
        checkpointSpout7.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout7.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap22);
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.activate();
        checkpointSpout24.activate();
        java.lang.Object obj27 = null;
        checkpointSpout24.fail(obj27);
        checkpointSpout24.close();
        checkpointSpout24.activate();
        checkpointSpout24.fail((java.lang.Object) 'a');
        checkpointSpout24.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout24.getComponentConfiguration();
        checkpointSpout24.deactivate();
        checkpointSpout24.deactivate();
        checkpointSpout24.fail((java.lang.Object) (-1L));
        checkpointSpout24.fail((java.lang.Object) (-1.0f));
        checkpointSpout24.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout24);
        checkpointSpout24.close();
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test1329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1329");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test1330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1330");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.activate();
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout33);
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout15.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass41 = strMap40.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test1331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1331");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.deactivate();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) 'a');
        checkpointSpout28.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout28.deactivate();
        checkpointSpout28.deactivate();
        checkpointSpout28.fail((java.lang.Object) (-1L));
        checkpointSpout28.fail((java.lang.Object) (-1.0f));
        checkpointSpout20.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout28.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout28);
        java.util.Map<java.lang.String, java.lang.Object> strMap48 = checkpointSpout28.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap48);
    }

    @Test
    public void test1332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1332");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.lang.Class<?> wildcardClass45 = checkpointSpout35.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass45);
    }

    @Test
    public void test1333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1333");
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
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1334");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.close();
        checkpointSpout22.activate();
        checkpointSpout22.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout22.getComponentConfiguration();
        checkpointSpout16.fail((java.lang.Object) strMap30);
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.deactivate();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout32.fail((java.lang.Object) checkpointSpout36);
        checkpointSpout32.deactivate();
        checkpointSpout32.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        org.junit.Assert.assertNull(strMap13);
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test1335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1335");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (-1L));
        checkpointSpout0.fail((java.lang.Object) (-1.0f));
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout25.getComponentConfiguration();
        checkpointSpout25.activate();
        checkpointSpout20.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout20.deactivate();
        java.lang.Class<?> wildcardClass34 = checkpointSpout20.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = null;
        org.apache.storm.task.TopologyContext topologyContext38 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector39 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap37, topologyContext38, spoutOutputCollector39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test1336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1336");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap9 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        checkpointSpout12.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout12.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout12);
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.fail((java.lang.Object) "action");
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.close();
        checkpointSpout28.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.deactivate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout35);
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.fail((java.lang.Object) "action");
        checkpointSpout42.activate();
        checkpointSpout42.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.activate();
        java.lang.Class<?> wildcardClass49 = checkpointSpout42.getClass();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout22.fail((java.lang.Object) checkpointSpout42);
        checkpointSpout12.fail((java.lang.Object) checkpointSpout22);
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap11);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNotNull(wildcardClass49);
    }

    @Test
    public void test1337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1337");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout35.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        java.lang.Object obj49 = null;
        checkpointSpout46.fail(obj49);
        checkpointSpout46.close();
        checkpointSpout46.close();
        checkpointSpout46.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.close();
        checkpointSpout54.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout61 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout61.activate();
        checkpointSpout61.activate();
        java.lang.Object obj64 = null;
        checkpointSpout61.fail(obj64);
        checkpointSpout61.deactivate();
        checkpointSpout54.fail((java.lang.Object) checkpointSpout61);
        checkpointSpout61.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap69 = checkpointSpout61.getComponentConfiguration();
        checkpointSpout46.fail((java.lang.Object) strMap69);
        checkpointSpout46.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap72 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap74 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.close();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout46);
        java.lang.Object obj77 = null;
        checkpointSpout46.fail(obj77);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap69);
        org.junit.Assert.assertNull(strMap72);
        org.junit.Assert.assertNull(strMap74);
    }

    @Test
    public void test1338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1338");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout15.fail((java.lang.Object) 'a');
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.deactivate();
        checkpointSpout15.deactivate();
        checkpointSpout15.fail((java.lang.Object) (-1L));
        checkpointSpout15.fail((java.lang.Object) (-1.0f));
        checkpointSpout7.fail((java.lang.Object) checkpointSpout15);
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout15.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.fail((java.lang.Object) "action");
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        checkpointSpout35.deactivate();
        checkpointSpout35.activate();
        checkpointSpout35.close();
        checkpointSpout35.close();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout35);
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout35.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap49 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout46.deactivate();
        checkpointSpout46.activate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout46);
        org.apache.storm.spout.CheckpointSpout checkpointSpout53 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout53.activate();
        checkpointSpout53.activate();
        java.lang.Object obj56 = null;
        checkpointSpout53.fail(obj56);
        checkpointSpout53.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout59 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout59.activate();
        checkpointSpout59.activate();
        java.lang.Object obj62 = null;
        checkpointSpout59.fail(obj62);
        checkpointSpout59.close();
        checkpointSpout59.activate();
        checkpointSpout59.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap67 = checkpointSpout59.getComponentConfiguration();
        checkpointSpout53.fail((java.lang.Object) strMap67);
        checkpointSpout53.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout53.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout71 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout71.activate();
        checkpointSpout71.activate();
        java.lang.Object obj74 = null;
        checkpointSpout71.fail(obj74);
        checkpointSpout71.close();
        checkpointSpout71.activate();
        checkpointSpout71.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap79 = checkpointSpout71.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap80 = checkpointSpout71.getComponentConfiguration();
        checkpointSpout53.fail((java.lang.Object) checkpointSpout71);
        checkpointSpout53.activate();
        checkpointSpout35.fail((java.lang.Object) checkpointSpout53);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap47);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap67);
        org.junit.Assert.assertNull(strMap70);
        org.junit.Assert.assertNull(strMap79);
        org.junit.Assert.assertNull(strMap80);
    }

    @Test
    public void test1339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1339");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.fail((java.lang.Object) "action");
        checkpointSpout11.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        java.lang.Object obj18 = null;
        checkpointSpout15.fail(obj18);
        checkpointSpout15.close();
        checkpointSpout15.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout22 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout22.activate();
        checkpointSpout22.activate();
        java.lang.Object obj25 = null;
        checkpointSpout22.fail(obj25);
        checkpointSpout22.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout11.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.activate();
        checkpointSpout15.deactivate();
        checkpointSpout15.close();
        java.lang.Class<?> wildcardClass33 = checkpointSpout15.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout15.deactivate();
        org.junit.Assert.assertNotNull(wildcardClass33);
    }

    @Test
    public void test1340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1340");
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
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test1341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1341");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        java.lang.Object obj31 = null;
        checkpointSpout28.fail(obj31);
        checkpointSpout28.deactivate();
        checkpointSpout21.fail((java.lang.Object) checkpointSpout28);
        checkpointSpout28.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout36 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout36.activate();
        checkpointSpout36.activate();
        java.lang.Object obj39 = null;
        checkpointSpout36.fail(obj39);
        checkpointSpout36.close();
        checkpointSpout36.activate();
        checkpointSpout36.fail((java.lang.Object) 'a');
        checkpointSpout36.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout36.deactivate();
        checkpointSpout36.deactivate();
        checkpointSpout36.fail((java.lang.Object) (-1L));
        checkpointSpout36.fail((java.lang.Object) (-1.0f));
        checkpointSpout28.fail((java.lang.Object) checkpointSpout36);
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout36.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap55 = checkpointSpout36.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout56 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout56.fail((java.lang.Object) "action");
        checkpointSpout56.activate();
        checkpointSpout56.activate();
        checkpointSpout56.deactivate();
        checkpointSpout56.activate();
        checkpointSpout56.close();
        checkpointSpout56.close();
        checkpointSpout36.fail((java.lang.Object) checkpointSpout56);
        checkpointSpout56.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout67 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout67.activate();
        checkpointSpout67.activate();
        java.lang.Object obj70 = null;
        checkpointSpout67.fail(obj70);
        checkpointSpout67.close();
        checkpointSpout67.activate();
        checkpointSpout67.fail((java.lang.Object) 'a');
        java.lang.Class<?> wildcardClass76 = checkpointSpout67.getClass();
        checkpointSpout56.fail((java.lang.Object) wildcardClass76);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout56);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap54);
        org.junit.Assert.assertNull(strMap55);
        org.junit.Assert.assertNotNull(wildcardClass76);
    }

    @Test
    public void test1342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1342");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.deactivate();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout6.deactivate();
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout6.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass31 = checkpointSpout23.getClass();
        checkpointSpout6.fail((java.lang.Object) wildcardClass31);
        checkpointSpout6.deactivate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout6);
        org.apache.storm.spout.CheckpointSpout checkpointSpout35 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout35.activate();
        checkpointSpout35.activate();
        java.lang.Object obj38 = null;
        checkpointSpout35.fail(obj38);
        checkpointSpout35.close();
        checkpointSpout35.activate();
        checkpointSpout35.fail((java.lang.Object) 'a');
        checkpointSpout35.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout35.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap47 = checkpointSpout35.getComponentConfiguration();
        checkpointSpout6.fail((java.lang.Object) strMap47);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNull(strMap29);
        org.junit.Assert.assertNotNull(wildcardClass31);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNull(strMap47);
    }

    @Test
    public void test1343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1343");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.deactivate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout16);
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.activate();
        java.lang.Class<?> wildcardClass30 = checkpointSpout23.getClass();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout23);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout9);
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout9.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = checkpointSpout33.getComponentConfiguration();
        java.lang.Class<?> wildcardClass46 = checkpointSpout33.getClass();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap45);
        org.junit.Assert.assertNotNull(wildcardClass46);
    }

    @Test
    public void test1344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1344");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.deactivate();
        checkpointSpout4.fail((java.lang.Object) checkpointSpout11);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout4);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test1345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1345");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout12.fail((java.lang.Object) strMap26);
        org.apache.storm.spout.CheckpointSpout checkpointSpout28 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout28.fail((java.lang.Object) "action");
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        checkpointSpout28.activate();
        checkpointSpout28.fail((java.lang.Object) (short) -1);
        checkpointSpout28.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout37 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout37.activate();
        checkpointSpout37.activate();
        java.lang.Object obj40 = null;
        checkpointSpout37.fail(obj40);
        checkpointSpout37.close();
        checkpointSpout37.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.deactivate();
        checkpointSpout37.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout37.activate();
        checkpointSpout28.fail((java.lang.Object) checkpointSpout37);
        checkpointSpout12.fail((java.lang.Object) checkpointSpout28);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test1346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1346");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout17);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout10.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout27.close();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout27);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test1347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1347");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.fail((java.lang.Object) (-1));
        checkpointSpout0.activate();
        checkpointSpout0.close();
    }

    @Test
    public void test1348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1348");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        java.lang.Object obj6 = null;
        checkpointSpout3.fail(obj6);
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.fail((java.lang.Object) 'a');
        checkpointSpout3.close();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        checkpointSpout3.deactivate();
        checkpointSpout3.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.deactivate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout25.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        java.lang.Object obj36 = null;
        checkpointSpout33.fail(obj36);
        checkpointSpout33.close();
        checkpointSpout33.activate();
        checkpointSpout33.fail((java.lang.Object) 'a');
        checkpointSpout33.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.deactivate();
        checkpointSpout33.deactivate();
        checkpointSpout33.fail((java.lang.Object) (-1L));
        checkpointSpout33.fail((java.lang.Object) (-1.0f));
        checkpointSpout25.fail((java.lang.Object) checkpointSpout33);
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout25.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.fail((java.lang.Object) "action");
        checkpointSpout52.activate();
        checkpointSpout52.deactivate();
        checkpointSpout52.fail((java.lang.Object) ' ');
        checkpointSpout52.fail((java.lang.Object) (-1));
        checkpointSpout52.activate();
        java.lang.Class<?> wildcardClass62 = checkpointSpout52.getClass();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout3.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout52.close();
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap51);
        org.junit.Assert.assertNotNull(wildcardClass62);
    }

    @Test
    public void test1349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1349");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.fail((java.lang.Object) "action");
        checkpointSpout6.activate();
        checkpointSpout6.close();
        checkpointSpout6.deactivate();
        checkpointSpout6.fail((java.lang.Object) (byte) -1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        checkpointSpout14.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.deactivate();
        checkpointSpout14.fail((java.lang.Object) checkpointSpout21);
        checkpointSpout6.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
    }
}

