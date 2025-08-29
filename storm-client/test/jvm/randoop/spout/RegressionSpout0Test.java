package randoop.spout;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionSpout0Test {

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
    public void test0001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0001");
        java.lang.String str0 = org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_ACTION;
        org.junit.Assert.assertEquals("'" + str0 + "' != '" + "action" + "'", str0, "action");
    }

    @Test
    public void test0002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0002");
        org.apache.storm.tuple.Tuple tuple0 = null;
        // The following exception was thrown during execution in test generation
        try {
            boolean boolean1 = org.apache.storm.spout.CheckpointSpout.isCheckpoint(tuple0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.tuple.Tuple.getSourceStreamId()\" because \"input\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0003");
        java.lang.String str0 = org.apache.storm.spout.CheckpointSpout.CHECKPOINT_STREAM_ID;
        org.junit.Assert.assertEquals("'" + str0 + "' != '" + "$checkpoint" + "'", str0, "$checkpoint");
    }

    @Test
    public void test0004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0004");
        java.lang.String str0 = org.apache.storm.spout.CheckpointSpout.CHECKPOINT_COMPONENT_ID;
        org.junit.Assert.assertEquals("'" + str0 + "' != '" + "$checkpointspout" + "'", str0, "$checkpointspout");
    }

    @Test
    public void test0005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0005");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
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
    public void test0006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0006");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        java.lang.Object obj6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0007");
        java.lang.String str0 = org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_TXID;
        org.junit.Assert.assertEquals("'" + str0 + "' != '" + "txid" + "'", str0, "txid");
    }

    @Test
    public void test0008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0008");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = null;
        org.apache.storm.task.TopologyContext topologyContext7 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap6, topologyContext7, spoutOutputCollector8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0009");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        org.apache.storm.spout.CheckpointSpout checkpointSpout1 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout1.activate();
        checkpointSpout1.activate();
        java.lang.Object obj4 = null;
        checkpointSpout1.fail(obj4);
        checkpointSpout1.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0010");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0011");
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
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0012");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0013");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) "txid");
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0014");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        java.lang.Object obj2 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj2);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0015");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass10 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0016");
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
    public void test0017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0017");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0018");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0019");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0020");
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
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0021");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
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
    public void test0022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0022");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = null;
        org.apache.storm.task.TopologyContext topologyContext4 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap3, topologyContext4, spoutOutputCollector5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0023");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 1.0d);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0024");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0025");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) (byte) -1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0026");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = null;
        org.apache.storm.task.TopologyContext topologyContext17 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap16, topologyContext17, spoutOutputCollector18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0027");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout15.fail((java.lang.Object) "action");
        checkpointSpout15.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test0028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0028");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0029");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0030");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0031");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Class<?> wildcardClass9 = checkpointSpout6.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0032");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.activate();
        checkpointSpout5.activate();
        java.lang.Object obj8 = null;
        checkpointSpout5.fail(obj8);
        checkpointSpout5.close();
        checkpointSpout5.activate();
        checkpointSpout5.fail((java.lang.Object) 'a');
        checkpointSpout5.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = checkpointSpout5.getComponentConfiguration();
        checkpointSpout5.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test0033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0033");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.fail((java.lang.Object) "action");
        checkpointSpout15.activate();
        checkpointSpout15.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout15.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) strMap20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test0034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0034");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = null;
        org.apache.storm.task.TopologyContext topologyContext2 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector3 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap1, topologyContext2, spoutOutputCollector3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0035");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0036");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0037");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 1.0d);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0038");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass7 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0039");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0040");
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
    public void test0041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0041");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0042");
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
    public void test0043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0043");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0044");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0045");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
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
    }

    @Test
    public void test0046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0046");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) (short) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0047");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0048");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0049");
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
        java.lang.Object obj10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test0050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0050");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
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
    public void test0051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0051");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = null;
        org.apache.storm.task.TopologyContext topologyContext4 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap3, topologyContext4, spoutOutputCollector5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap2);
    }

    @Test
    public void test0052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0052");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
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
    }

    @Test
    public void test0053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0053");
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
    public void test0054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0054");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.fail((java.lang.Object) "action");
        checkpointSpout12.activate();
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
        checkpointSpout12.fail((java.lang.Object) checkpointSpout16);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0055");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0056");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
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
    public void test0057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0057");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0058");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0059");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass10 = strMap9.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test0060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0060");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0061");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
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
    public void test0062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0062");
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
    public void test0063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0063");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
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
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0064");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.close();
        checkpointSpout11.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0065");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0066");
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
        java.lang.Class<?> wildcardClass17 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0067");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
    }

    @Test
    public void test0068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0068");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0069");
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
        java.lang.Class<?> wildcardClass14 = checkpointSpout7.getClass();
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0070");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0071");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0072");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0073");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0074");
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
        java.lang.Class<?> wildcardClass15 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0075");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = null;
        org.apache.storm.task.TopologyContext topologyContext20 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector21 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.open(strMap19, topologyContext20, spoutOutputCollector21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0076");
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
        java.lang.Class<?> wildcardClass40 = checkpointSpout15.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass40);
    }

    @Test
    public void test0077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0077");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test0078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0078");
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
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        checkpointSpout11.fail((java.lang.Object) 'a');
        checkpointSpout11.close();
        checkpointSpout11.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0079");
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
        java.lang.Class<?> wildcardClass11 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0080");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0081");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
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
    }

    @Test
    public void test0082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0082");
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
        java.lang.Class<?> wildcardClass16 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0083");
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
    }

    @Test
    public void test0084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0084");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.declareOutputFields(outputFieldsDeclarer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0085");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
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
    public void test0086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0086");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.deactivate();
        java.lang.Class<?> wildcardClass12 = checkpointSpout6.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNotNull(wildcardClass12);
    }

    @Test
    public void test0087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0087");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0088");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer33 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer33);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test0089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0089");
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
        checkpointSpout7.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0090");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0091");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0092");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0093");
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
    public void test0094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0094");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0095");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0096");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = null;
        org.apache.storm.task.TopologyContext topologyContext18 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap17, topologyContext18, spoutOutputCollector19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0097");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap2);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0098");
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
        checkpointSpout7.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) 0.0f);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0099");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = null;
        org.apache.storm.task.TopologyContext topologyContext21 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector22 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap20, topologyContext21, spoutOutputCollector22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0100");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = null;
        org.apache.storm.task.TopologyContext topologyContext34 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap33, topologyContext34, spoutOutputCollector35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test0101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0101");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = null;
        org.apache.storm.task.TopologyContext topologyContext34 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.open(strMap33, topologyContext34, spoutOutputCollector35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test0102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0102");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer26 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.declareOutputFields(outputFieldsDeclarer26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0103");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = null;
        org.apache.storm.task.TopologyContext topologyContext41 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector42 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.open(strMap40, topologyContext41, spoutOutputCollector42);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0104");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.declareOutputFields(outputFieldsDeclarer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0105");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
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
    public void test0106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0106");
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
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) 'a');
        checkpointSpout16.close();
        checkpointSpout16.fail((java.lang.Object) 0.0f);
        java.lang.Class<?> wildcardClass28 = checkpointSpout16.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test0107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0107");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
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
    public void test0108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0108");
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
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test0109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0109");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test0110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0110");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0111");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0112");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.fail((java.lang.Object) 100.0d);
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0113");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass7 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0114");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 1.0d);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0115");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.declareOutputFields(outputFieldsDeclarer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0116");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test0117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0117");
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
        checkpointSpout8.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0118");
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
    public void test0119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0119");
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
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test0120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0120");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass3 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0121");
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
    public void test0122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0122");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0123");
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
        java.lang.Class<?> wildcardClass11 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0124");
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
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        checkpointSpout7.close();
        checkpointSpout7.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout7.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test0125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0125");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0126");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0127");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.deactivate();
        java.lang.Class<?> wildcardClass29 = checkpointSpout23.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout14.ack((java.lang.Object) wildcardClass29);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
        org.junit.Assert.assertNotNull(wildcardClass29);
    }

    @Test
    public void test0128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0128");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0129");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        java.lang.Class<?> wildcardClass11 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0130");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap11 = checkpointSpout10.getComponentConfiguration();
        checkpointSpout10.deactivate();
        checkpointSpout10.activate();
        checkpointSpout10.close();
        checkpointSpout10.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test0131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0131");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0132");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0133");
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
        java.lang.Class<?> wildcardClass30 = checkpointSpout20.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0134");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0135");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0136");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0137");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout14.ack((java.lang.Object) (-1.0f));
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0138");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass27 = strMap26.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test0139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0139");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test0140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0140");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0141");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass2 = strMap1.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0142");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = null;
        org.apache.storm.task.TopologyContext topologyContext27 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector28 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.open(strMap26, topologyContext27, spoutOutputCollector28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0143");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.activate();
        checkpointSpout3.activate();
        java.lang.Object obj6 = null;
        checkpointSpout3.fail(obj6);
        checkpointSpout3.close();
        checkpointSpout3.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.deactivate();
        checkpointSpout3.fail((java.lang.Object) checkpointSpout10);
        checkpointSpout3.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout3.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0144");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass8 = checkpointSpout7.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout7);
        checkpointSpout7.close();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0145");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.fail((java.lang.Object) "action");
        checkpointSpout34.fail((java.lang.Object) "action");
        checkpointSpout34.deactivate();
        checkpointSpout34.close();
        java.lang.Class<?> wildcardClass51 = checkpointSpout34.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack((java.lang.Object) wildcardClass51);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNotNull(wildcardClass51);
    }

    @Test
    public void test0146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0146");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = null;
        org.apache.storm.task.TopologyContext topologyContext21 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector22 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap20, topologyContext21, spoutOutputCollector22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0147");
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
    public void test0148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0148");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        java.lang.Object obj21 = null;
        checkpointSpout18.fail(obj21);
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.fail((java.lang.Object) 'a');
        checkpointSpout18.close();
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout18);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0149");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
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
    }

    @Test
    public void test0150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0150");
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
        java.lang.Class<?> wildcardClass21 = checkpointSpout15.getClass();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0151");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0152");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        checkpointSpout50.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) checkpointSpout50);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test0153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0153");
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
        java.lang.Class<?> wildcardClass15 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0154");
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
    public void test0155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0155");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer27 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.declareOutputFields(outputFieldsDeclarer27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0156");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass19 = strMap18.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test0157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0157");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = null;
        org.apache.storm.task.TopologyContext topologyContext15 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.open(strMap14, topologyContext15, spoutOutputCollector16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0158");
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
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0159");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout55 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout55.activate();
        checkpointSpout55.activate();
        java.lang.Object obj58 = null;
        checkpointSpout55.fail(obj58);
        checkpointSpout55.close();
        checkpointSpout55.activate();
        checkpointSpout55.fail((java.lang.Object) 'a');
        checkpointSpout55.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap65 = checkpointSpout55.getComponentConfiguration();
        checkpointSpout55.deactivate();
        checkpointSpout55.deactivate();
        checkpointSpout55.fail((java.lang.Object) (-1L));
        checkpointSpout55.fail((java.lang.Object) (-1.0f));
        checkpointSpout47.fail((java.lang.Object) checkpointSpout55);
        java.util.Map<java.lang.String, java.lang.Object> strMap73 = checkpointSpout47.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack((java.lang.Object) strMap73);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap65);
        org.junit.Assert.assertNull(strMap73);
    }

    @Test
    public void test0160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0160");
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
        checkpointSpout16.close();
        checkpointSpout16.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test0161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0161");
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
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass14 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0162");
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
    public void test0163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0163");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
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
        org.junit.Assert.assertNull(strMap1);
    }

    @Test
    public void test0164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0164");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
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
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test0165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0165");
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
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test0166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0166");
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
        java.lang.Class<?> wildcardClass50 = checkpointSpout7.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNotNull(wildcardClass50);
    }

    @Test
    public void test0167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0167");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        checkpointSpout17.fail((java.lang.Object) 10.0d);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 10.0d);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0168");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test0169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0169");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.fail((java.lang.Object) 'a');
        checkpointSpout16.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.deactivate();
        checkpointSpout16.deactivate();
        checkpointSpout16.fail((java.lang.Object) (-1L));
        checkpointSpout16.fail((java.lang.Object) (-1.0f));
        checkpointSpout16.close();
        checkpointSpout16.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test0170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0170");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
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
    public void test0171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0171");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0172");
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
    public void test0173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0173");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0174");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0175");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0176");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer50 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout41.declareOutputFields(outputFieldsDeclarer50);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test0177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0177");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
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
    }

    @Test
    public void test0178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0178");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.fail((java.lang.Object) "action");
        checkpointSpout19.activate();
        checkpointSpout19.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout24 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout24.fail((java.lang.Object) "action");
        checkpointSpout24.activate();
        checkpointSpout24.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap29 = checkpointSpout24.getComponentConfiguration();
        checkpointSpout24.activate();
        checkpointSpout19.fail((java.lang.Object) checkpointSpout24);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test0179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0179");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0180");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.close();
        checkpointSpout43.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout50.activate();
        checkpointSpout50.activate();
        java.lang.Object obj53 = null;
        checkpointSpout50.fail(obj53);
        checkpointSpout50.deactivate();
        checkpointSpout43.fail((java.lang.Object) checkpointSpout50);
        checkpointSpout50.deactivate();
        checkpointSpout16.fail((java.lang.Object) checkpointSpout50);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap42);
    }

    @Test
    public void test0181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0181");
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
        checkpointSpout22.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout29 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout29.activate();
        checkpointSpout29.activate();
        java.lang.Object obj32 = null;
        checkpointSpout29.fail(obj32);
        checkpointSpout29.deactivate();
        checkpointSpout22.fail((java.lang.Object) checkpointSpout29);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0182");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test0183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0183");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass3 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0184");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0185");
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
        checkpointSpout26.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout26);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0186");
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
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer34 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer34);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0187");
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
        checkpointSpout7.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout15.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.ack((java.lang.Object) strMap16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0188");
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
    public void test0189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0189");
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
        java.lang.Object obj23 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0190");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer40 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout21.declareOutputFields(outputFieldsDeclarer40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0191");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass9 = strMap8.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test0192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0192");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0193");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0194");
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
        checkpointSpout53.fail((java.lang.Object) "action");
        checkpointSpout53.activate();
        checkpointSpout53.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout58 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout58.fail((java.lang.Object) "action");
        checkpointSpout58.activate();
        checkpointSpout58.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap63 = checkpointSpout58.getComponentConfiguration();
        checkpointSpout58.activate();
        checkpointSpout53.fail((java.lang.Object) checkpointSpout58);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.ack((java.lang.Object) checkpointSpout58);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap63);
    }

    @Test
    public void test0195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0195");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0196");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
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
    public void test0197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0197");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0198");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        java.lang.Object obj40 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack(obj40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0199");
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
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0200");
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
        checkpointSpout7.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer36 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0201");
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
    public void test0202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0202");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0203");
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
        checkpointSpout7.activate();
        checkpointSpout7.fail((java.lang.Object) 'a');
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 'a');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0204");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test0205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0205");
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
        checkpointSpout7.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0206");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout50 = new org.apache.storm.spout.CheckpointSpout();
        java.lang.Class<?> wildcardClass51 = checkpointSpout50.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout41.ack((java.lang.Object) checkpointSpout50);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNotNull(wildcardClass51);
    }

    @Test
    public void test0207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0207");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.fail((java.lang.Object) 'a');
        checkpointSpout6.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout6.deactivate();
        checkpointSpout6.deactivate();
        checkpointSpout6.fail((java.lang.Object) (-1L));
        checkpointSpout6.fail((java.lang.Object) (-1.0f));
        checkpointSpout6.close();
        checkpointSpout6.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0208");
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
        java.lang.Class<?> wildcardClass35 = checkpointSpout15.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test0209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0209");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = null;
        org.apache.storm.task.TopologyContext topologyContext26 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector27 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap25, topologyContext26, spoutOutputCollector27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test0210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0210");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.deactivate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout25);
        java.util.Map<java.lang.String, java.lang.Object> strMap32 = checkpointSpout18.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.activate();
        checkpointSpout33.deactivate();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout18.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap32);
    }

    @Test
    public void test0211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0211");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.fail((java.lang.Object) (-1));
        checkpointSpout0.activate();
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
    public void test0212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0212");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout12 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        java.lang.Object obj15 = null;
        checkpointSpout12.fail(obj15);
        checkpointSpout12.close();
        checkpointSpout12.activate();
        checkpointSpout12.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap11);
    }

    @Test
    public void test0213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0213");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout6.declareOutputFields(outputFieldsDeclarer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0214");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0215");
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
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout34 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout34.activate();
        checkpointSpout34.activate();
        java.lang.Object obj37 = null;
        checkpointSpout34.fail(obj37);
        checkpointSpout34.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.close();
        checkpointSpout44.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.activate();
        checkpointSpout51.activate();
        java.lang.Object obj54 = null;
        checkpointSpout51.fail(obj54);
        checkpointSpout51.deactivate();
        checkpointSpout44.fail((java.lang.Object) checkpointSpout51);
        checkpointSpout40.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout34.fail((java.lang.Object) checkpointSpout44);
        checkpointSpout44.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0216");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.activate();
        checkpointSpout21.fail((java.lang.Object) 'a');
        java.lang.Class<?> wildcardClass30 = checkpointSpout21.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass30);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0217");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test0218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0218");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        java.lang.Object obj4 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0219");
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
    public void test0220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0220");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.activate();
        checkpointSpout15.close();
        checkpointSpout15.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0221");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap2 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer4 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap2);
    }

    @Test
    public void test0222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0222");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.lang.Object obj10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test0223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0223");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0224");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = null;
        org.apache.storm.task.TopologyContext topologyContext7 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap6, topologyContext7, spoutOutputCollector8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0225");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0226");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack((java.lang.Object) 10.0f);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0227");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNotNull(wildcardClass37);
    }

    @Test
    public void test0228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0228");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0229");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.fail((java.lang.Object) 'a');
        checkpointSpout27.close();
        java.lang.Class<?> wildcardClass37 = checkpointSpout27.getClass();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout27);
        checkpointSpout27.deactivate();
        org.junit.Assert.assertNotNull(wildcardClass37);
    }

    @Test
    public void test0230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0230");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test0231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0231");
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
    public void test0232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0232");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0233");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.close();
        checkpointSpout16.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0234");
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
    }

    @Test
    public void test0235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0235");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0236");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0237");
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
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0238");
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
    public void test0239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0239");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass4 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNotNull(wildcardClass4);
    }

    @Test
    public void test0240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0240");
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
        java.lang.Class<?> wildcardClass24 = checkpointSpout10.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test0241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0241");
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
        checkpointSpout0.activate();
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
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0242");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        java.lang.Object obj47 = null;
        checkpointSpout44.fail(obj47);
        checkpointSpout44.close();
        checkpointSpout44.close();
        checkpointSpout44.close();
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
        checkpointSpout59.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap67 = checkpointSpout59.getComponentConfiguration();
        checkpointSpout44.fail((java.lang.Object) strMap67);
        checkpointSpout44.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap70 = checkpointSpout44.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout9.ack((java.lang.Object) strMap70);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap67);
        org.junit.Assert.assertNull(strMap70);
    }

    @Test
    public void test0243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0243");
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
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0244");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.close();
        checkpointSpout40.activate();
        checkpointSpout40.fail((java.lang.Object) 'a');
        checkpointSpout40.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout40.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout51 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout51.fail((java.lang.Object) "action");
        checkpointSpout40.fail((java.lang.Object) "action");
        checkpointSpout40.deactivate();
        checkpointSpout40.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout40.getComponentConfiguration();
        java.lang.Class<?> wildcardClass58 = checkpointSpout40.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack((java.lang.Object) wildcardClass58);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap57);
        org.junit.Assert.assertNotNull(wildcardClass58);
    }

    @Test
    public void test0245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0245");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
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
    }

    @Test
    public void test0246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0246");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0247");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = null;
        org.apache.storm.task.TopologyContext topologyContext34 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap33, topologyContext34, spoutOutputCollector35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0248");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0249");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
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
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test0250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0250");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        java.lang.Object obj12 = null;
        checkpointSpout9.fail(obj12);
        checkpointSpout9.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout15 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout15.fail((java.lang.Object) "action");
        checkpointSpout15.activate();
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
        checkpointSpout15.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout19);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0251");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0252");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
    }

    @Test
    public void test0253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0253");
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
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass24 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass24);
    }

    @Test
    public void test0254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0254");
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
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) "action");
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0255");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0256");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0257");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0258");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) 10.0d);
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0259");
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
        checkpointSpout0.fail((java.lang.Object) "action");
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = null;
        org.apache.storm.task.TopologyContext topologyContext26 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector27 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap25, topologyContext26, spoutOutputCollector27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0260");
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
        checkpointSpout0.close();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0261");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0262");
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
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 1.0d);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0263");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass28 = strMap27.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test0264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0264");
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
        java.lang.Class<?> wildcardClass13 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0265");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer32 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test0266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0266");
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
    public void test0267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0267");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0268");
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
    }

    @Test
    public void test0269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0269");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0270");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        checkpointSpout14.activate();
        checkpointSpout14.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout14.getComponentConfiguration();
        checkpointSpout8.fail((java.lang.Object) strMap22);
        checkpointSpout8.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = checkpointSpout8.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        checkpointSpout26.activate();
        checkpointSpout26.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout26.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout26);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0271");
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
        checkpointSpout0.close();
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0272");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0273");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.lang.Object obj7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack(obj7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0274");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test0275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0275");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0276");
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
    public void test0277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0277");
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
    public void test0278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0278");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0279");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
    }

    @Test
    public void test0280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0280");
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
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.fail((java.lang.Object) "action");
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        checkpointSpout17.activate();
        checkpointSpout17.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
        checkpointSpout25.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout30 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout30.fail((java.lang.Object) "action");
        checkpointSpout30.activate();
        checkpointSpout30.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout30.getComponentConfiguration();
        checkpointSpout30.activate();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout30);
        checkpointSpout25.deactivate();
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
        checkpointSpout39.close();
        checkpointSpout25.fail((java.lang.Object) checkpointSpout39);
        checkpointSpout17.fail((java.lang.Object) checkpointSpout25);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test0281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0281");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = null;
        org.apache.storm.task.TopologyContext topologyContext7 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap6, topologyContext7, spoutOutputCollector8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0282");
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
        checkpointSpout26.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout26.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout26);
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout26.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer37 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.declareOutputFields(outputFieldsDeclarer37);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0283");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0284");
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
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0285");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0286");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout6 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout6.activate();
        checkpointSpout6.activate();
        java.lang.Object obj9 = null;
        checkpointSpout6.fail(obj9);
        checkpointSpout6.close();
        checkpointSpout6.activate();
        checkpointSpout6.fail((java.lang.Object) 'a');
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 'a');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0287");
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
        java.lang.Class<?> wildcardClass34 = checkpointSpout7.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test0288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0288");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0289");
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
        checkpointSpout16.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout16.deactivate();
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test0290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0290");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0291");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout27.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap35);
        checkpointSpout21.close();
        checkpointSpout21.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout21);
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = null;
        org.apache.storm.task.TopologyContext topologyContext41 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector42 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout21.open(strMap40, topologyContext41, spoutOutputCollector42);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0292");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer28 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer28);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test0293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0293");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer22 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0294");
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
        checkpointSpout16.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass35 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test0295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0295");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer6 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0296");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0297");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0298");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = null;
        org.apache.storm.task.TopologyContext topologyContext39 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector40 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.open(strMap38, topologyContext39, spoutOutputCollector40);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0299");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.close();
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0300");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer51 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout41.declareOutputFields(outputFieldsDeclarer51);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0301");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
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
        checkpointSpout15.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap30 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap31 = checkpointSpout15.getComponentConfiguration();
        checkpointSpout15.close();
        checkpointSpout15.activate();
        checkpointSpout11.fail((java.lang.Object) checkpointSpout15);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout15);
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNull(strMap31);
    }

    @Test
    public void test0302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0302");
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
        java.lang.Class<?> wildcardClass38 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test0303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0303");
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
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0304");
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
        java.lang.Class<?> wildcardClass35 = checkpointSpout7.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test0305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0305");
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
        checkpointSpout26.fail((java.lang.Object) "action");
        checkpointSpout26.activate();
        checkpointSpout26.close();
        checkpointSpout26.deactivate();
        checkpointSpout26.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.ack((java.lang.Object) checkpointSpout26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0306");
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
        java.lang.Class<?> wildcardClass32 = checkpointSpout14.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass32);
    }

    @Test
    public void test0307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0307");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = null;
        org.apache.storm.task.TopologyContext topologyContext24 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector25 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout5.open(strMap23, topologyContext24, spoutOutputCollector25);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0308");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0309");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap27);
    }

    @Test
    public void test0310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0310");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap13 = null;
        org.apache.storm.task.TopologyContext topologyContext14 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector15 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout5.open(strMap13, topologyContext14, spoutOutputCollector15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0311");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.close();
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
    }

    @Test
    public void test0312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0312");
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
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0313");
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
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0314");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap26);
    }

    @Test
    public void test0315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0315");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap25 = null;
        org.apache.storm.task.TopologyContext topologyContext26 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector27 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap25, topologyContext26, spoutOutputCollector27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0316");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0317");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass9 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0318");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0319");
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
        java.lang.Class<?> wildcardClass16 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0320");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
    }

    @Test
    public void test0321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0321");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout3.declareOutputFields(outputFieldsDeclarer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0322");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0323");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.fail((java.lang.Object) "action");
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        checkpointSpout20.deactivate();
        checkpointSpout20.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0324");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer51 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer51);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0325");
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
    public void test0326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0326");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer3 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer3);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0327");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap4 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap4);
    }

    @Test
    public void test0328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0328");
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
        checkpointSpout23.activate();
        java.lang.Object obj26 = null;
        checkpointSpout23.fail(obj26);
        checkpointSpout23.close();
        checkpointSpout23.activate();
        checkpointSpout23.fail((java.lang.Object) 'a');
        checkpointSpout23.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap33 = checkpointSpout23.getComponentConfiguration();
        checkpointSpout23.deactivate();
        checkpointSpout23.deactivate();
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
        java.util.Map<java.lang.String, java.lang.Object> strMap69 = checkpointSpout51.getComponentConfiguration();
        checkpointSpout23.fail((java.lang.Object) checkpointSpout51);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout51);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout51.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap61);
        org.junit.Assert.assertNull(strMap69);
    }

    @Test
    public void test0329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0329");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer32 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout14.declareOutputFields(outputFieldsDeclarer32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test0330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0330");
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
        checkpointSpout3.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout3.declareOutputFields(outputFieldsDeclarer17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0331");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.activate();
        checkpointSpout21.activate();
        java.lang.Object obj24 = null;
        checkpointSpout21.fail(obj24);
        checkpointSpout21.close();
        checkpointSpout21.close();
        checkpointSpout21.close();
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
        checkpointSpout36.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = checkpointSpout36.getComponentConfiguration();
        checkpointSpout21.fail((java.lang.Object) strMap44);
        checkpointSpout21.close();
        java.lang.Class<?> wildcardClass47 = checkpointSpout21.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.ack((java.lang.Object) checkpointSpout21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNotNull(wildcardClass47);
    }

    @Test
    public void test0332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0332");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.activate();
        checkpointSpout13.activate();
        java.lang.Object obj16 = null;
        checkpointSpout13.fail(obj16);
        checkpointSpout13.close();
        checkpointSpout13.activate();
        checkpointSpout13.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout13.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout13.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout23 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout23.fail((java.lang.Object) "action");
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        checkpointSpout23.activate();
        checkpointSpout23.fail((java.lang.Object) (short) -1);
        checkpointSpout23.close();
        checkpointSpout23.close();
        checkpointSpout13.fail((java.lang.Object) checkpointSpout23);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test0333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0333");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = null;
        org.apache.storm.task.TopologyContext topologyContext18 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap17, topologyContext18, spoutOutputCollector19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0334");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        java.lang.Object obj49 = null;
        checkpointSpout46.fail(obj49);
        checkpointSpout46.close();
        checkpointSpout46.activate();
        checkpointSpout46.fail((java.lang.Object) 'a');
        java.lang.Class<?> wildcardClass55 = checkpointSpout46.getClass();
        checkpointSpout35.fail((java.lang.Object) wildcardClass55);
        checkpointSpout35.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass55);
    }

    @Test
    public void test0335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0335");
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
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0336");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0337");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout17 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout17.activate();
        checkpointSpout17.activate();
        java.lang.Object obj20 = null;
        checkpointSpout17.fail(obj20);
        checkpointSpout17.close();
        checkpointSpout17.activate();
        checkpointSpout17.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0338");
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
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0339");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout9.declareOutputFields(outputFieldsDeclarer35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0340");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout7 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        java.lang.Object obj10 = null;
        checkpointSpout7.fail(obj10);
        checkpointSpout7.close();
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
        checkpointSpout7.fail((java.lang.Object) checkpointSpout17);
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
        java.util.Map<java.lang.String, java.lang.Object> strMap50 = checkpointSpout33.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap51 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout17.fail((java.lang.Object) checkpointSpout33);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout33);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
    }

    @Test
    public void test0341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0341");
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
        checkpointSpout0.fail((java.lang.Object) false);
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = null;
        org.apache.storm.task.TopologyContext topologyContext17 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap16, topologyContext17, spoutOutputCollector18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0342");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        checkpointSpout8.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout8.deactivate();
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
        checkpointSpout8.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer42 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer42);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0343");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        checkpointSpout8.activate();
        checkpointSpout8.fail((java.lang.Object) 'a');
        checkpointSpout8.close();
        java.lang.Class<?> wildcardClass18 = checkpointSpout8.getClass();
        checkpointSpout0.fail((java.lang.Object) wildcardClass18);
        java.lang.Class<?> wildcardClass20 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass18);
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0344");
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
        checkpointSpout41.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0345");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap10 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0346");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer22 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer22);
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
    public void test0347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0347");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        checkpointSpout32.fail((java.lang.Object) 'a');
        checkpointSpout32.close();
        java.lang.Class<?> wildcardClass42 = checkpointSpout32.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout32);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass42);
    }

    @Test
    public void test0348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0348");
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
        java.lang.Class<?> wildcardClass18 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNotNull(wildcardClass18);
    }

    @Test
    public void test0349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0349");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.activate();
        checkpointSpout10.fail((java.lang.Object) 'a');
        checkpointSpout10.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = checkpointSpout10.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.fail((java.lang.Object) "action");
        checkpointSpout10.fail((java.lang.Object) "action");
        checkpointSpout10.deactivate();
        checkpointSpout10.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap27 = checkpointSpout10.getComponentConfiguration();
        java.lang.Class<?> wildcardClass28 = checkpointSpout10.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNotNull(wildcardClass28);
    }

    @Test
    public void test0350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0350");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout2 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout2.activate();
        checkpointSpout2.activate();
        java.lang.Object obj5 = null;
        checkpointSpout2.fail(obj5);
        checkpointSpout2.close();
        checkpointSpout2.activate();
        checkpointSpout2.fail((java.lang.Object) 'a');
        checkpointSpout2.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap12 = checkpointSpout2.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout2.fail((java.lang.Object) "action");
        checkpointSpout2.deactivate();
        checkpointSpout2.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout2.getComponentConfiguration();
        java.lang.Class<?> wildcardClass20 = checkpointSpout2.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass20);
    }

    @Test
    public void test0351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0351");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) 'a');
        org.apache.storm.spout.CheckpointSpout checkpointSpout9 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout9.fail((java.lang.Object) "action");
        checkpointSpout9.activate();
        checkpointSpout9.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap14 = checkpointSpout9.getComponentConfiguration();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        checkpointSpout9.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout18.fail((java.lang.Object) "action");
        checkpointSpout18.activate();
        checkpointSpout18.activate();
        checkpointSpout18.deactivate();
        checkpointSpout18.activate();
        checkpointSpout18.close();
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
        checkpointSpout26.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout40 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout40.activate();
        checkpointSpout40.activate();
        java.lang.Object obj43 = null;
        checkpointSpout40.fail(obj43);
        checkpointSpout40.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        java.lang.Object obj49 = null;
        checkpointSpout46.fail(obj49);
        checkpointSpout46.close();
        checkpointSpout46.activate();
        checkpointSpout46.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap54 = checkpointSpout46.getComponentConfiguration();
        checkpointSpout40.fail((java.lang.Object) strMap54);
        checkpointSpout40.close();
        checkpointSpout26.fail((java.lang.Object) checkpointSpout40);
        checkpointSpout18.fail((java.lang.Object) checkpointSpout26);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout26);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap54);
    }

    @Test
    public void test0352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0352");
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
        java.lang.Class<?> wildcardClass12 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass12);
    }

    @Test
    public void test0353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0353");
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
        checkpointSpout0.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.activate();
        checkpointSpout25.activate();
        java.lang.Object obj28 = null;
        checkpointSpout25.fail(obj28);
        checkpointSpout25.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout31 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout31.fail((java.lang.Object) "action");
        checkpointSpout31.activate();
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
        checkpointSpout31.fail((java.lang.Object) checkpointSpout35);
        checkpointSpout25.fail((java.lang.Object) checkpointSpout35);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0354");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) 'a');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0355");
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
        java.lang.Class<?> wildcardClass19 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0356");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass13 = strMap12.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap12);
    }

    @Test
    public void test0357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0357");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout16 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout16.activate();
        checkpointSpout16.activate();
        java.lang.Object obj19 = null;
        checkpointSpout16.fail(obj19);
        checkpointSpout16.close();
        checkpointSpout16.activate();
        checkpointSpout16.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout16.getComponentConfiguration();
        checkpointSpout16.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap24);
    }

    @Test
    public void test0358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0358");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        checkpointSpout8.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout8.deactivate();
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
        checkpointSpout8.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0359");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        java.lang.Class<?> wildcardClass8 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0360");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer18 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0361");
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
        checkpointSpout3.deactivate();
        java.lang.Class<?> wildcardClass18 = checkpointSpout3.getClass();
        org.junit.Assert.assertNotNull(wildcardClass18);
    }

    @Test
    public void test0362() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0362");
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
        java.lang.Class<?> wildcardClass15 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0363() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0363");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout4 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout4.activate();
        checkpointSpout4.activate();
        java.lang.Object obj7 = null;
        checkpointSpout4.fail(obj7);
        checkpointSpout4.close();
        checkpointSpout4.activate();
        checkpointSpout4.fail((java.lang.Object) 'a');
        checkpointSpout4.activate();
        checkpointSpout4.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout4);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
    }

    @Test
    public void test0364() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0364");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout46 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout46.activate();
        checkpointSpout46.activate();
        java.lang.Object obj49 = null;
        checkpointSpout46.fail(obj49);
        checkpointSpout46.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.fail((java.lang.Object) "action");
        checkpointSpout52.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout56 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout56.activate();
        checkpointSpout56.activate();
        java.lang.Object obj59 = null;
        checkpointSpout56.fail(obj59);
        checkpointSpout56.close();
        checkpointSpout56.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout63 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout63.activate();
        checkpointSpout63.activate();
        java.lang.Object obj66 = null;
        checkpointSpout63.fail(obj66);
        checkpointSpout63.deactivate();
        checkpointSpout56.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout52.fail((java.lang.Object) checkpointSpout56);
        checkpointSpout46.fail((java.lang.Object) checkpointSpout56);
        checkpointSpout56.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap73 = checkpointSpout56.getComponentConfiguration();
        checkpointSpout35.fail((java.lang.Object) strMap73);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout35.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap73);
    }

    @Test
    public void test0365() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0365");
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
        checkpointSpout0.fail((java.lang.Object) false);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap27);
        org.junit.Assert.assertNull(strMap45);
    }

    @Test
    public void test0366() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0366");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer21 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0367() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0367");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        java.lang.Object obj22 = null;
        checkpointSpout19.fail(obj22);
        checkpointSpout19.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout25 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout25.fail((java.lang.Object) "action");
        checkpointSpout25.activate();
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
        checkpointSpout25.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout19.fail((java.lang.Object) checkpointSpout29);
        checkpointSpout29.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout29.getComponentConfiguration();
        checkpointSpout0.fail((java.lang.Object) strMap46);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer48 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer48);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test0368() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0368");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer9 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test0369() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0369");
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
        checkpointSpout19.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = checkpointSpout19.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap35 = checkpointSpout19.getComponentConfiguration();
        checkpointSpout19.close();
        checkpointSpout19.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout19);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout19.declareOutputFields(outputFieldsDeclarer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap35);
    }

    @Test
    public void test0370() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0370");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout47.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0371() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0371");
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
        java.lang.Class<?> wildcardClass34 = checkpointSpout15.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNotNull(wildcardClass34);
    }

    @Test
    public void test0372() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0372");
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
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) (byte) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0373() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0373");
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
        checkpointSpout0.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0374() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0374");
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
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0375() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0375");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout5 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout5.activate();
        checkpointSpout5.activate();
        java.lang.Object obj8 = null;
        checkpointSpout5.fail(obj8);
        checkpointSpout5.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout11 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout5.fail((java.lang.Object) strMap19);
        org.apache.storm.spout.CheckpointSpout checkpointSpout21 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout21.fail((java.lang.Object) "action");
        checkpointSpout21.activate();
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
        checkpointSpout21.fail((java.lang.Object) checkpointSpout25);
        checkpointSpout21.deactivate();
        checkpointSpout21.deactivate();
        checkpointSpout5.fail((java.lang.Object) checkpointSpout21);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout21);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0376() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0376");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap7 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap7);
    }

    @Test
    public void test0377() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0377");
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
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap16 = checkpointSpout0.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0378() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0378");
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
        checkpointSpout33.close();
        java.lang.Class<?> wildcardClass54 = checkpointSpout33.getClass();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNotNull(wildcardClass54);
    }

    @Test
    public void test0379() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0379");
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
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test0380() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0380");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0381() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0381");
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
        checkpointSpout16.close();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout16);
        java.util.Map<java.lang.String, java.lang.Object> strMap34 = null;
        org.apache.storm.task.TopologyContext topologyContext35 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector36 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.open(strMap34, topologyContext35, spoutOutputCollector36);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap30);
    }

    @Test
    public void test0382() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0382");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer20 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0383() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0383");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout27 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout27.activate();
        checkpointSpout27.activate();
        java.lang.Object obj30 = null;
        checkpointSpout27.fail(obj30);
        checkpointSpout27.close();
        checkpointSpout27.activate();
        checkpointSpout27.fail((java.lang.Object) 'a');
        checkpointSpout27.close();
        java.lang.Class<?> wildcardClass37 = checkpointSpout27.getClass();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout27);
        java.lang.Class<?> wildcardClass39 = checkpointSpout10.getClass();
        org.junit.Assert.assertNotNull(wildcardClass37);
        org.junit.Assert.assertNotNull(wildcardClass39);
    }

    @Test
    public void test0384() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0384");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout18.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNotNull(wildcardClass19);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test0385() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0385");
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
        java.lang.Class<?> wildcardClass14 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass14);
    }

    @Test
    public void test0386() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0386");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer42 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.declareOutputFields(outputFieldsDeclarer42);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap41);
    }

    @Test
    public void test0387() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0387");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (byte) 1);
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0388() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0388");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.activate();
        checkpointSpout8.activate();
        java.lang.Object obj11 = null;
        checkpointSpout8.fail(obj11);
        checkpointSpout8.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout14 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        java.lang.Object obj17 = null;
        checkpointSpout14.fail(obj17);
        checkpointSpout14.close();
        checkpointSpout14.close();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout14);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer23 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
    }

    @Test
    public void test0389() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0389");
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
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0390() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0390");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        checkpointSpout0.close();
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
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout4.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) strMap18);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap18);
    }

    @Test
    public void test0391() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0391");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0392() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0392");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass9 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0393() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0393");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0394() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0394");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0395() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0395");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.fail((java.lang.Object) "action");
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout19.activate();
        checkpointSpout19.fail((java.lang.Object) (byte) 1);
        java.lang.Class<?> wildcardClass27 = checkpointSpout19.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test0396() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0396");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer24 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.declareOutputFields(outputFieldsDeclarer24);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0397() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0397");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
    }

    @Test
    public void test0398() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0398");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        checkpointSpout8.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout8.deactivate();
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
        checkpointSpout8.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        checkpointSpout8.activate();
        java.lang.Class<?> wildcardClass42 = checkpointSpout8.getClass();
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNotNull(wildcardClass42);
    }

    @Test
    public void test0399() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0399");
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
        java.lang.Class<?> wildcardClass38 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
        org.junit.Assert.assertNotNull(wildcardClass38);
    }

    @Test
    public void test0400() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0400");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer39 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer39);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0401() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0401");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = null;
        org.apache.storm.task.TopologyContext topologyContext9 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector10 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap8, topologyContext9, spoutOutputCollector10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0402() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0402");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout26 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout26.activate();
        checkpointSpout26.activate();
        java.lang.Object obj29 = null;
        checkpointSpout26.fail(obj29);
        checkpointSpout26.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.activate();
        checkpointSpout32.activate();
        java.lang.Object obj35 = null;
        checkpointSpout32.fail(obj35);
        checkpointSpout32.close();
        checkpointSpout32.activate();
        checkpointSpout32.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout32.getComponentConfiguration();
        checkpointSpout26.fail((java.lang.Object) strMap40);
        checkpointSpout26.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = checkpointSpout26.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap40);
        org.junit.Assert.assertNull(strMap43);
    }

    @Test
    public void test0403() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0403");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0404() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0404");
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
        java.lang.Class<?> wildcardClass89 = checkpointSpout42.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap60);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNull(strMap85);
        org.junit.Assert.assertNotNull(wildcardClass89);
    }

    @Test
    public void test0405() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0405");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap45 = null;
        org.apache.storm.task.TopologyContext topologyContext46 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector47 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.open(strMap45, topologyContext46, spoutOutputCollector47);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test0406() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0406");
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
        checkpointSpout14.activate();
        checkpointSpout14.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0407() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0407");
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
        checkpointSpout33.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test0408() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0408");
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
    }

    @Test
    public void test0409() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0409");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer16 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer16);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
    }

    @Test
    public void test0410() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0410");
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
        checkpointSpout0.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout19 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout19.fail((java.lang.Object) "action");
        checkpointSpout19.activate();
        checkpointSpout19.close();
        checkpointSpout19.deactivate();
        checkpointSpout19.fail((java.lang.Object) (byte) -1);
        checkpointSpout19.activate();
        checkpointSpout19.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0411() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0411");
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
    public void test0412() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0412");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer54 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.declareOutputFields(outputFieldsDeclarer54);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
    }

    @Test
    public void test0413() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0413");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        checkpointSpout8.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout8.deactivate();
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
        checkpointSpout8.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = null;
        org.apache.storm.task.TopologyContext topologyContext42 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector43 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.open(strMap41, topologyContext42, spoutOutputCollector43);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0414() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0414");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap20 = null;
        org.apache.storm.task.TopologyContext topologyContext21 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector22 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.open(strMap20, topologyContext21, spoutOutputCollector22);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0415() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0415");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
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
    public void test0416() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0416");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer11 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0417() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0417");
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
        checkpointSpout11.activate();
        checkpointSpout11.activate();
        java.lang.Object obj14 = null;
        checkpointSpout11.fail(obj14);
        checkpointSpout11.close();
        checkpointSpout11.activate();
        checkpointSpout11.fail((java.lang.Object) 'a');
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap26 = checkpointSpout11.getComponentConfiguration();
        checkpointSpout11.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = checkpointSpout11.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap26);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test0418() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0418");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap32 = null;
        org.apache.storm.task.TopologyContext topologyContext33 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector34 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout14.open(strMap32, topologyContext33, spoutOutputCollector34);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
    }

    @Test
    public void test0419() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0419");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout8 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout8.fail((java.lang.Object) "action");
        checkpointSpout8.activate();
        checkpointSpout8.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout13 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout13.fail((java.lang.Object) "action");
        checkpointSpout13.activate();
        checkpointSpout13.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap18 = checkpointSpout13.getComponentConfiguration();
        checkpointSpout13.activate();
        checkpointSpout8.fail((java.lang.Object) checkpointSpout13);
        checkpointSpout8.deactivate();
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
        checkpointSpout8.fail((java.lang.Object) checkpointSpout22);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout8);
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout8.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap18);
        org.junit.Assert.assertNull(strMap36);
        org.junit.Assert.assertNull(strMap41);
    }

    @Test
    public void test0420() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0420");
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
        // The following exception was thrown during execution in test generation
        try {
            java.lang.Class<?> wildcardClass18 = strMap17.getClass();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0421() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0421");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = null;
        org.apache.storm.task.TopologyContext topologyContext47 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector48 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.open(strMap46, topologyContext47, spoutOutputCollector48);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test0422() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0422");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap1 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap3 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer5 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer5);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
    }

    @Test
    public void test0423() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0423");
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
        java.lang.Class<?> wildcardClass26 = checkpointSpout12.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) wildcardClass26);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNotNull(wildcardClass26);
    }

    @Test
    public void test0424() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0424");
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
    public void test0425() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0425");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass8 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass8);
    }

    @Test
    public void test0426() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0426");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer35 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.declareOutputFields(outputFieldsDeclarer35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0427() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0427");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer7 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0428() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0428");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap44 = null;
        org.apache.storm.task.TopologyContext topologyContext45 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector46 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout9.open(strMap44, topologyContext45, spoutOutputCollector46);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
    }

    @Test
    public void test0429() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0429");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0430() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0430");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) ' ');
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
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
    public void test0431() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0431");
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
        java.lang.Class<?> wildcardClass23 = checkpointSpout5.getClass();
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test0432() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0432");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout11.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass32);
        org.junit.Assert.assertNull(strMap50);
        org.junit.Assert.assertNull(strMap51);
    }

    @Test
    public void test0433() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0433");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap88 = null;
        org.apache.storm.task.TopologyContext topologyContext89 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector90 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout42.open(strMap88, topologyContext89, spoutOutputCollector90);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
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
    public void test0434() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0434");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = null;
        org.apache.storm.task.TopologyContext topologyContext18 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector19 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap17, topologyContext18, spoutOutputCollector19);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0435() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0435");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer41 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.declareOutputFields(outputFieldsDeclarer41);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNotNull(wildcardClass37);
    }

    @Test
    public void test0436() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0436");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout15.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = null;
        org.apache.storm.task.TopologyContext topologyContext43 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector44 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.open(strMap42, topologyContext43, spoutOutputCollector44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap40);
        org.junit.Assert.assertNull(strMap41);
    }

    @Test
    public void test0437() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0437");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass25 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0438() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0438");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.close();
        checkpointSpout43.activate();
        checkpointSpout43.fail((java.lang.Object) 'a');
        checkpointSpout43.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap53 = checkpointSpout43.getComponentConfiguration();
        checkpointSpout43.deactivate();
        checkpointSpout43.deactivate();
        checkpointSpout43.fail((java.lang.Object) (-1L));
        checkpointSpout43.fail((java.lang.Object) (-1.0f));
        checkpointSpout43.close();
        checkpointSpout43.activate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.ack((java.lang.Object) checkpointSpout43);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap53);
    }

    @Test
    public void test0439() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0439");
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
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test0440() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0440");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
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
    public void test0441() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0441");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap66 = null;
        org.apache.storm.task.TopologyContext topologyContext67 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector68 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout61.open(strMap66, topologyContext67, spoutOutputCollector68);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap62);
    }

    @Test
    public void test0442() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0442");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap46 = checkpointSpout26.getComponentConfiguration();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer47 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout26.declareOutputFields(outputFieldsDeclarer47);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
        org.junit.Assert.assertNull(strMap46);
    }

    @Test
    public void test0443() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0443");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.close();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass10 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass10);
    }

    @Test
    public void test0444() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0444");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer27 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer27);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0445() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0445");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer52 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout7.declareOutputFields(outputFieldsDeclarer52);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0446() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0446");
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
        checkpointSpout46.deactivate();
        checkpointSpout46.deactivate();
        checkpointSpout10.fail((java.lang.Object) checkpointSpout46);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout10.ack((java.lang.Object) (byte) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap41);
        org.junit.Assert.assertNull(strMap42);
        org.junit.Assert.assertNull(strMap43);
        org.junit.Assert.assertNull(strMap44);
    }

    @Test
    public void test0447() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0447");
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
        checkpointSpout0.close();
        checkpointSpout0.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0448() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0448");
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
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = null;
        org.apache.storm.task.TopologyContext topologyContext24 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector25 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap23, topologyContext24, spoutOutputCollector25);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0449() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0449");
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
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap65);
        org.junit.Assert.assertNotNull(wildcardClass67);
        org.junit.Assert.assertNull(strMap85);
        org.junit.Assert.assertNull(strMap86);
    }

    @Test
    public void test0450() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0450");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap5 = checkpointSpout0.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap6 = null;
        org.apache.storm.task.TopologyContext topologyContext7 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector8 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap6, topologyContext7, spoutOutputCollector8);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap5);
    }

    @Test
    public void test0451() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0451");
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
        checkpointSpout0.close();
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
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0452() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0452");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        checkpointSpout0.deactivate();
        org.junit.Assert.assertNull(strMap17);
    }

    @Test
    public void test0453() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0453");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0454() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0454");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout59 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout59.activate();
        checkpointSpout59.activate();
        java.lang.Object obj62 = null;
        checkpointSpout59.fail(obj62);
        checkpointSpout59.close();
        checkpointSpout59.activate();
        checkpointSpout59.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap67 = checkpointSpout59.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout59.getComponentConfiguration();
        checkpointSpout59.activate();
        java.lang.Class<?> wildcardClass70 = checkpointSpout59.getClass();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout53.ack((java.lang.Object) wildcardClass70);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap67);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNotNull(wildcardClass70);
    }

    @Test
    public void test0455() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0455");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer14 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer14);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap8);
    }

    @Test
    public void test0456() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0456");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout8.ack((java.lang.Object) checkpointSpout23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test0457() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0457");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap36 = checkpointSpout20.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap30);
        org.junit.Assert.assertNotNull(wildcardClass34);
        org.junit.Assert.assertNull(strMap36);
    }

    @Test
    public void test0458() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0458");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout44 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout44.fail((java.lang.Object) "action");
        checkpointSpout44.activate();
        checkpointSpout44.activate();
        checkpointSpout44.deactivate();
        checkpointSpout44.activate();
        checkpointSpout44.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout52 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout52.fail((java.lang.Object) "action");
        checkpointSpout52.activate();
        checkpointSpout52.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout57 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout57.fail((java.lang.Object) "action");
        checkpointSpout57.activate();
        checkpointSpout57.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap62 = checkpointSpout57.getComponentConfiguration();
        checkpointSpout57.activate();
        checkpointSpout52.fail((java.lang.Object) checkpointSpout57);
        checkpointSpout52.deactivate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout66 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout66.activate();
        checkpointSpout66.activate();
        java.lang.Object obj69 = null;
        checkpointSpout66.fail(obj69);
        checkpointSpout66.close();
        org.apache.storm.spout.CheckpointSpout checkpointSpout72 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout72.activate();
        checkpointSpout72.activate();
        java.lang.Object obj75 = null;
        checkpointSpout72.fail(obj75);
        checkpointSpout72.close();
        checkpointSpout72.activate();
        checkpointSpout72.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap80 = checkpointSpout72.getComponentConfiguration();
        checkpointSpout66.fail((java.lang.Object) strMap80);
        checkpointSpout66.close();
        checkpointSpout52.fail((java.lang.Object) checkpointSpout66);
        checkpointSpout44.fail((java.lang.Object) checkpointSpout52);
        checkpointSpout44.close();
        checkpointSpout44.deactivate();
        checkpointSpout44.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout33.ack((java.lang.Object) checkpointSpout44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap62);
        org.junit.Assert.assertNull(strMap80);
    }

    @Test
    public void test0459() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0459");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        org.apache.storm.spout.CheckpointSpout checkpointSpout3 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout3.fail((java.lang.Object) "action");
        checkpointSpout3.activate();
        checkpointSpout3.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap8 = checkpointSpout3.getComponentConfiguration();
        checkpointSpout3.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout10 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout10.activate();
        checkpointSpout10.activate();
        java.lang.Object obj13 = null;
        checkpointSpout10.fail(obj13);
        checkpointSpout10.close();
        checkpointSpout10.close();
        checkpointSpout10.deactivate();
        checkpointSpout10.close();
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout33 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout33.fail((java.lang.Object) "action");
        checkpointSpout33.activate();
        checkpointSpout33.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap38 = checkpointSpout33.getComponentConfiguration();
        checkpointSpout33.activate();
        java.lang.Class<?> wildcardClass40 = checkpointSpout33.getClass();
        checkpointSpout19.fail((java.lang.Object) checkpointSpout33);
        checkpointSpout10.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout3.fail((java.lang.Object) checkpointSpout19);
        checkpointSpout0.fail((java.lang.Object) checkpointSpout3);
        org.apache.storm.spout.CheckpointSpout checkpointSpout45 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout45.activate();
        checkpointSpout45.activate();
        java.lang.Object obj48 = null;
        checkpointSpout45.fail(obj48);
        checkpointSpout45.close();
        checkpointSpout45.close();
        checkpointSpout45.close();
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
        checkpointSpout60.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap68 = checkpointSpout60.getComponentConfiguration();
        checkpointSpout45.fail((java.lang.Object) strMap68);
        checkpointSpout45.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap71 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout45.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap73 = checkpointSpout45.getComponentConfiguration();
        checkpointSpout3.fail((java.lang.Object) strMap73);
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNotNull(wildcardClass40);
        org.junit.Assert.assertNull(strMap68);
        org.junit.Assert.assertNull(strMap71);
        org.junit.Assert.assertNull(strMap73);
    }

    @Test
    public void test0460() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0460");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout19.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap29);
    }

    @Test
    public void test0461() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0461");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = checkpointSpout5.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout5.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap23);
    }

    @Test
    public void test0462() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0462");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        java.lang.Object obj46 = null;
        checkpointSpout43.fail(obj46);
        checkpointSpout43.close();
        checkpointSpout43.activate();
        checkpointSpout43.fail((java.lang.Object) 'a');
        checkpointSpout43.close();
        checkpointSpout43.activate();
        checkpointSpout43.activate();
        checkpointSpout40.fail((java.lang.Object) checkpointSpout43);
        checkpointSpout43.deactivate();
        checkpointSpout43.deactivate();
        checkpointSpout15.fail((java.lang.Object) checkpointSpout43);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout43.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0463() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0463");
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
        java.lang.Class<?> wildcardClass56 = checkpointSpout47.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNotNull(wildcardClass56);
    }

    @Test
    public void test0464() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0464");
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
        checkpointSpout0.close();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer13 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer13);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0465() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0465");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap58 = null;
        org.apache.storm.task.TopologyContext topologyContext59 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector60 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap58, topologyContext59, spoutOutputCollector60);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNull(strMap56);
    }

    @Test
    public void test0466() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0466");
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
        checkpointSpout25.activate();
        org.junit.Assert.assertNull(strMap23);
        org.junit.Assert.assertNull(strMap35);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test0467() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0467");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.close();
        checkpointSpout0.activate();
        java.lang.Class<?> wildcardClass9 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0468() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0468");
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
        checkpointSpout70.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap93 = null;
        org.apache.storm.task.TopologyContext topologyContext94 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector95 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout70.open(strMap93, topologyContext94, spoutOutputCollector95);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap65);
        org.junit.Assert.assertNotNull(wildcardClass67);
        org.junit.Assert.assertNull(strMap85);
        org.junit.Assert.assertNull(strMap86);
    }

    @Test
    public void test0469() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0469");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout3.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0470() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0470");
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
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0471() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0471");
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
        checkpointSpout31.fail((java.lang.Object) "action");
        checkpointSpout31.activate();
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
        checkpointSpout31.fail((java.lang.Object) checkpointSpout35);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout35);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap12);
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test0472() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0472");
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
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer15 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer15);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0473() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0473");
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
            checkpointSpout4.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
    }

    @Test
    public void test0474() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0474");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.deactivate();
        checkpointSpout0.deactivate();
        checkpointSpout0.activate();
    }

    @Test
    public void test0475() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0475");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.lang.Object obj3 = null;
        checkpointSpout0.fail(obj3);
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
        java.util.Map<java.lang.String, java.lang.Object> strMap21 = checkpointSpout6.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap22 = checkpointSpout6.getComponentConfiguration();
        checkpointSpout6.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap24 = checkpointSpout6.getComponentConfiguration();
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
        checkpointSpout32.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap40 = checkpointSpout32.getComponentConfiguration();
        checkpointSpout6.fail((java.lang.Object) checkpointSpout32);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout6);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap21);
        org.junit.Assert.assertNull(strMap22);
        org.junit.Assert.assertNull(strMap24);
        org.junit.Assert.assertNull(strMap40);
    }

    @Test
    public void test0476() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0476");
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
        checkpointSpout53.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap59 = checkpointSpout53.getComponentConfiguration();
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass30);
        org.junit.Assert.assertNull(strMap48);
        org.junit.Assert.assertNull(strMap49);
        org.junit.Assert.assertNull(strMap59);
    }

    @Test
    public void test0477() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0477");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap28 = null;
        org.apache.storm.task.TopologyContext topologyContext29 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector30 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout4.open(strMap28, topologyContext29, spoutOutputCollector30);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
    }

    @Test
    public void test0478() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0478");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap42 = null;
        org.apache.storm.task.TopologyContext topologyContext43 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector44 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap42, topologyContext43, spoutOutputCollector44);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0479() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0479");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.close();
        checkpointSpout20.close();
        checkpointSpout20.activate();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout20);
        checkpointSpout0.activate();
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer31 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer31);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNotNull(wildcardClass16);
    }

    @Test
    public void test0480() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0480");
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
        checkpointSpout9.close();
        checkpointSpout9.deactivate();
        checkpointSpout9.close();
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout32 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout32.fail((java.lang.Object) "action");
        checkpointSpout32.activate();
        checkpointSpout32.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap37 = checkpointSpout32.getComponentConfiguration();
        checkpointSpout32.activate();
        java.lang.Class<?> wildcardClass39 = checkpointSpout32.getClass();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout32);
        checkpointSpout9.fail((java.lang.Object) checkpointSpout18);
        org.apache.storm.spout.CheckpointSpout checkpointSpout42 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout42.activate();
        checkpointSpout42.activate();
        java.lang.Object obj45 = null;
        checkpointSpout42.fail(obj45);
        checkpointSpout42.close();
        checkpointSpout42.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout49 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout49.activate();
        checkpointSpout49.activate();
        java.lang.Object obj52 = null;
        checkpointSpout49.fail(obj52);
        checkpointSpout49.deactivate();
        checkpointSpout42.fail((java.lang.Object) checkpointSpout49);
        checkpointSpout42.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap57 = checkpointSpout42.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap58 = checkpointSpout42.getComponentConfiguration();
        checkpointSpout42.close();
        checkpointSpout42.close();
        checkpointSpout18.fail((java.lang.Object) checkpointSpout42);
        org.apache.storm.spout.CheckpointSpout checkpointSpout62 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout62.activate();
        checkpointSpout62.activate();
        checkpointSpout62.deactivate();
        checkpointSpout42.fail((java.lang.Object) checkpointSpout62);
        checkpointSpout62.close();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout62);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap6);
        org.junit.Assert.assertNull(strMap8);
        org.junit.Assert.assertNull(strMap37);
        org.junit.Assert.assertNotNull(wildcardClass39);
        org.junit.Assert.assertNull(strMap57);
        org.junit.Assert.assertNull(strMap58);
    }

    @Test
    public void test0481() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0481");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout20 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout20.activate();
        checkpointSpout20.activate();
        java.lang.Object obj23 = null;
        checkpointSpout20.fail(obj23);
        checkpointSpout20.close();
        checkpointSpout20.activate();
        checkpointSpout20.fail((java.lang.Object) 'a');
        checkpointSpout20.close();
        checkpointSpout20.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout20);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap15);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap19);
    }

    @Test
    public void test0482() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0482");
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
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer12 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.declareOutputFields(outputFieldsDeclarer12);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap9);
        org.junit.Assert.assertNull(strMap10);
    }

    @Test
    public void test0483() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0483");
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
        checkpointSpout14.deactivate();
        java.lang.Class<?> wildcardClass32 = checkpointSpout14.getClass();
        checkpointSpout5.fail((java.lang.Object) checkpointSpout14);
        org.apache.storm.topology.OutputFieldsDeclarer outputFieldsDeclarer34 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout5.declareOutputFields(outputFieldsDeclarer34);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.topology.OutputFieldsDeclarer.declareStream(String, org.apache.storm.tuple.Fields)\" because \"declarer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass32);
    }

    @Test
    public void test0484() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0484");
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
        checkpointSpout0.activate();
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap22);
    }

    @Test
    public void test0485() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0485");
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
        checkpointSpout7.deactivate();
        checkpointSpout7.activate();
        checkpointSpout7.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap34);
    }

    @Test
    public void test0486() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0486");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap15 = null;
        org.apache.storm.task.TopologyContext topologyContext16 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector17 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap15, topologyContext16, spoutOutputCollector17);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0487() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0487");
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
        checkpointSpout14.deactivate();
        java.lang.Class<?> wildcardClass32 = checkpointSpout14.getClass();
        checkpointSpout5.fail((java.lang.Object) checkpointSpout14);
        checkpointSpout5.activate();
        java.lang.Class<?> wildcardClass35 = checkpointSpout5.getClass();
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap28);
        org.junit.Assert.assertNotNull(wildcardClass32);
        org.junit.Assert.assertNotNull(wildcardClass35);
    }

    @Test
    public void test0488() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0488");
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
        checkpointSpout41.close();
        checkpointSpout41.activate();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap33);
        org.junit.Assert.assertNull(strMap50);
    }

    @Test
    public void test0489() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0489");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap41 = checkpointSpout15.getComponentConfiguration();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout15.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap40);
        org.junit.Assert.assertNull(strMap41);
    }

    @Test
    public void test0490() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0490");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout43 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout43.fail((java.lang.Object) "action");
        checkpointSpout43.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout47 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout47.activate();
        checkpointSpout47.activate();
        java.lang.Object obj50 = null;
        checkpointSpout47.fail(obj50);
        checkpointSpout47.close();
        checkpointSpout47.activate();
        org.apache.storm.spout.CheckpointSpout checkpointSpout54 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout54.activate();
        checkpointSpout54.activate();
        java.lang.Object obj57 = null;
        checkpointSpout54.fail(obj57);
        checkpointSpout54.deactivate();
        checkpointSpout47.fail((java.lang.Object) checkpointSpout54);
        checkpointSpout43.fail((java.lang.Object) checkpointSpout47);
        checkpointSpout37.fail((java.lang.Object) checkpointSpout47);
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
        checkpointSpout63.deactivate();
        java.util.Map<java.lang.String, java.lang.Object> strMap78 = checkpointSpout63.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap79 = checkpointSpout63.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap80 = checkpointSpout63.getComponentConfiguration();
        java.util.Map<java.lang.String, java.lang.Object> strMap81 = checkpointSpout63.getComponentConfiguration();
        checkpointSpout47.fail((java.lang.Object) checkpointSpout63);
        checkpointSpout63.activate();
        java.util.Map<java.lang.String, java.lang.Object> strMap84 = checkpointSpout63.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout85 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout85.fail((java.lang.Object) "action");
        checkpointSpout85.activate();
        checkpointSpout85.close();
        java.lang.Class<?> wildcardClass90 = checkpointSpout85.getClass();
        checkpointSpout63.fail((java.lang.Object) wildcardClass90);
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.ack((java.lang.Object) checkpointSpout63);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap34);
        org.junit.Assert.assertNull(strMap78);
        org.junit.Assert.assertNull(strMap79);
        org.junit.Assert.assertNull(strMap80);
        org.junit.Assert.assertNull(strMap81);
        org.junit.Assert.assertNull(strMap84);
        org.junit.Assert.assertNotNull(wildcardClass90);
    }

    @Test
    public void test0491() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0491");
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
        checkpointSpout0.deactivate();
        checkpointSpout0.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap17 = checkpointSpout0.getComponentConfiguration();
        org.apache.storm.spout.CheckpointSpout checkpointSpout18 = new org.apache.storm.spout.CheckpointSpout();
        java.util.Map<java.lang.String, java.lang.Object> strMap19 = checkpointSpout18.getComponentConfiguration();
        checkpointSpout18.close();
        java.lang.Class<?> wildcardClass21 = checkpointSpout18.getClass();
        checkpointSpout0.fail((java.lang.Object) checkpointSpout18);
        java.util.Map<java.lang.String, java.lang.Object> strMap23 = null;
        org.apache.storm.task.TopologyContext topologyContext24 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector25 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.open(strMap23, topologyContext24, spoutOutputCollector25);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0492() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0492");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap19);
        org.junit.Assert.assertNull(strMap20);
        org.junit.Assert.assertNull(strMap21);
    }

    @Test
    public void test0493() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0493");
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
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout0.nextTuple();
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.nextAction(boolean)\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap1);
        org.junit.Assert.assertNull(strMap3);
        org.junit.Assert.assertNull(strMap31);
        org.junit.Assert.assertNull(strMap39);
        org.junit.Assert.assertNull(strMap56);
    }

    @Test
    public void test0494() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0494");
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
    }

    @Test
    public void test0495() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0495");
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
        org.apache.storm.spout.CheckpointSpout checkpointSpout49 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout49.fail((java.lang.Object) "action");
        checkpointSpout49.activate();
        checkpointSpout49.activate();
        checkpointSpout49.close();
        checkpointSpout49.close();
        java.util.Map<java.lang.String, java.lang.Object> strMap56 = checkpointSpout49.getComponentConfiguration();
        checkpointSpout49.deactivate();
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout28.ack((java.lang.Object) checkpointSpout49);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.spout.CheckPointState.getTxid()\" because \"this.curTxState\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap10);
        org.junit.Assert.assertNull(strMap38);
        org.junit.Assert.assertNull(strMap46);
        org.junit.Assert.assertNull(strMap56);
    }

    @Test
    public void test0496() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0496");
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
        java.util.Map<java.lang.String, java.lang.Object> strMap43 = null;
        org.apache.storm.task.TopologyContext topologyContext44 = null;
        org.apache.storm.spout.SpoutOutputCollector spoutOutputCollector45 = null;
        // The following exception was thrown during execution in test generation
        try {
            checkpointSpout16.open(strMap43, topologyContext44, spoutOutputCollector45);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.util.Map.containsKey(Object)\" because \"topoConf\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNull(strMap14);
    }

    @Test
    public void test0497() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0497");
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
        java.lang.Class<?> wildcardClass19 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap14);
        org.junit.Assert.assertNull(strMap16);
        org.junit.Assert.assertNull(strMap17);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0498() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0498");
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
        java.lang.Class<?> wildcardClass30 = checkpointSpout0.getClass();
        org.junit.Assert.assertNull(strMap25);
        org.junit.Assert.assertNotNull(wildcardClass27);
        org.junit.Assert.assertNotNull(wildcardClass30);
    }

    @Test
    public void test0499() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0499");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.close();
        checkpointSpout0.deactivate();
        checkpointSpout0.fail((java.lang.Object) (byte) -1);
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
    public void test0500() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionSpout0Test.test0500");
        org.apache.storm.spout.CheckpointSpout checkpointSpout0 = new org.apache.storm.spout.CheckpointSpout();
        checkpointSpout0.fail((java.lang.Object) "action");
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.activate();
        checkpointSpout0.fail((java.lang.Object) (short) -1);
        checkpointSpout0.activate();
        checkpointSpout0.close();
        java.lang.Class<?> wildcardClass10 = checkpointSpout0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass10);
    }
}

