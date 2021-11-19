package org.apache.hadoop.yarn.applications.distributedshell;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestClient {
    @Test
    public void testParseResourcesString() {
        // Setup
        final Map<String, Long> expectedResult = new HashMap<>();
        expectedResult.put("memory-mb", 3072L);
        expectedResult.put("vcores", 1L);

        // Run the test
        final Map<String, Long> result = Client.parseResourcesString("[memory-mb=3072,vcores=1]");

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
