/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

public class TestRMDiagnosticJStackService {

  private static final int NUMBER_OF_JSTACKS = 3;
  private static final String DUMMY_JSTACK = "Full thread dump\n";

  @Test
  public void testCollectResourceManagerThreadDumpSuccess() throws IOException {
    ResourceManager rm = mock(ResourceManager.class);
    ApplicationACLsManager aclsManager = mock(ApplicationACLsManager.class);
    when(rm.getApplicationACLsManager()).thenReturn(aclsManager);
    when(aclsManager.isAdmin(any())).thenReturn(true);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn("yarn");

    RMDiagnosticJStackService service = new RMDiagnosticJStackService(rm);

    try (MockedConstruction<Shell.ShellCommandExecutor> mockedConstruction =
        mockConstruction(Shell.ShellCommandExecutor.class,
            (mock, context) -> when(mock.getOutput()).thenReturn(DUMMY_JSTACK))) {

      String result = service.collectResourceManagerThreadDump(
          NUMBER_OF_JSTACKS, mockRequest);

      assertEquals(NUMBER_OF_JSTACKS, mockedConstruction.constructed().size());
      assertTrue(result.contains(DUMMY_JSTACK));
      assertTrue(result.contains("JStack iteration 0"));
    }
  }

  @Test
  public void testCollectResourceManagerThreadDumpForbidden() {
    ResourceManager rm = mock(ResourceManager.class);
    ApplicationACLsManager aclsManager = mock(ApplicationACLsManager.class);
    when(rm.getApplicationACLsManager()).thenReturn(aclsManager);
    when(aclsManager.isAdmin(any())).thenReturn(false);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn("alice");

    RMDiagnosticJStackService service = new RMDiagnosticJStackService(rm);

    assertThrows(YarnRuntimeException.class,
        () -> service.collectResourceManagerThreadDump(1, mockRequest));
  }
}
