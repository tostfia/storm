package org.apache.storm.spout;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailTestLlm {
    private CheckpointSpout spout;

    @Before
    public void setup() {
        spout = new CheckpointSpout();
        setPrivateField(spout, "recovering", false);
    }

    @Test
    public void testFailSetsRecoveringTrue() {
        spout.fail(123L);
        assertTrue((Boolean) getPrivateField(spout, "recovering"));
    }

    @Test
    public void testFailDoesNotChangeRecoveringIfAlreadyTrue() {
        setPrivateField(spout, "recovering", true);
        spout.fail(123L);
        assertTrue((Boolean) getPrivateField(spout, "recovering"));
    }

    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            fail("Reflection error: " + e.getMessage());
        }
    }

    private Object getPrivateField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception e) {
            fail("Reflection error: " + e.getMessage());
            return null;
        }
    }
}