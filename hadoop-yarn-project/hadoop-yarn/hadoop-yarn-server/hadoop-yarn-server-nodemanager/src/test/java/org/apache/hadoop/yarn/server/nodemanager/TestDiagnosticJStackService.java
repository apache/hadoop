/** * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.server.nodemanager.webapp.DiagnosticJStackService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


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
