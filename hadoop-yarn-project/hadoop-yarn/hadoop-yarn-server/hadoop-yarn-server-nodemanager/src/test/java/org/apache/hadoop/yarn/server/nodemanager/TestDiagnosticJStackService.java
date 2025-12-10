package org.apache.hadoop.yarn.server.nodemanager;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestDiagnosticJStackService {

    @Test
    public void testScriptLocationShouldReturnExistingExecutableFile(){
        String scriptPath = callPrivateGetScriptLocation();
        assertNotNull(scriptPath, "Script location should not be null");
    }

    @Test
    public void testCreateProcessBuilderAppCommandNumJStackOption(){
        ProcessBuilder pb = DiagnosticJStackService
                .createProcessBuilder("5");

        List<String> cmd = pb.command();
        assertEquals(3, cmd.size());
        assertEquals("python3", cmd.get(0));
        assertTrue(cmd.get(1).contains("jstack_collector"), "Script path should contain jstack_collector");
        assertEquals("5", cmd.get(2));
    }

    @Test
    public void testCreateProcessBuilderAppCommandAppIdJStackOption(){
        ProcessBuilder pb = DiagnosticJStackService
                .createProcessBuilder("app_123", "5");

        List<String> cmd = pb.command();
        assertEquals(4, cmd.size());
        assertEquals("python3", cmd.get(0));
        assertTrue(cmd.get(1).contains("jstack_collector"), "Script path should contain jstack_collector");
        assertEquals("app_123", cmd.get(2));
        assertEquals("5", cmd.get(3));
    }

    private String callPrivateGetScriptLocation() {
        try {
            Method m = DiagnosticJStackService.class.getDeclaredMethod("getScriptLocation");
            m.setAccessible(true);
            return (String) m.invoke(null);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
