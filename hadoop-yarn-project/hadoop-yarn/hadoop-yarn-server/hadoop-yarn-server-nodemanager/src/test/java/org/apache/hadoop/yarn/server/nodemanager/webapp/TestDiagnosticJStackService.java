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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;


public class TestDiagnosticJStackService {

  private static final int NUMBER_OF_JSTACKS = 3;
  private static final String DUMMY_JSTACK =
      "Full thread dump OpenJDK 64-Bit Server VM (17.0.15+6-Ubuntu-0ubuntu120.04...";
  private static final String APPLICATION_ID_STR = "application_1771512066750_0001";
  private static final ApplicationId APPLICATION_ID =
      ApplicationId.fromString(APPLICATION_ID_STR);
  private static final String CONTAINER_ID_STR = "container_1771512066750_0001_01_000049";
  private static final ContainerId CONTAINER_ID =
      ContainerId.fromString(CONTAINER_ID_STR);
  private static final ApplicationACLsManager MOCK_ACLS_MANAGER =
      mock(ApplicationACLsManager.class);

  private static final NodeManager.NMContext NM_CONTEXT = new NodeManager.NMContext(
      null, null, null
      , MOCK_ACLS_MANAGER , null , false, new Configuration()
  );
  private static final DiagnosticJStackService DIAGNOSTIC_JSTACK_SERVICE =
      spy(new DiagnosticJStackService(NM_CONTEXT));


  @Test
  public void testWrongApplicationId() {
    String applicationId = "app_29042";

    assertThrows(RuntimeException.class,
         () -> DIAGNOSTIC_JSTACK_SERVICE.collectApplicationThreadDump(applicationId, 3, null));
  }

  @Test
  public void testCollectNodeThreadDumpSuccess() {
    // No need to mock ProcessID, as it will take the unit test JVM PID
    try(MockedConstruction<Shell.ShellCommandExecutor> mockedConstruction =
        mockConstruction(Shell.ShellCommandExecutor.class,
          (mock, context) -> when(mock.getOutput()).thenReturn(DUMMY_JSTACK))
      // Wrap mockConstruction here to automatically close it
    ){
      HttpServletRequest mockRequest = mock(HttpServletRequest.class);
      when(MOCK_ACLS_MANAGER.isAdmin(any())).thenReturn(true);

      String result =
        DIAGNOSTIC_JSTACK_SERVICE.collectNodeThreadDump(NUMBER_OF_JSTACKS, mockRequest);

      assertEquals(NUMBER_OF_JSTACKS, mockedConstruction.constructed().size(),
          "ShellCommandExecutor should be instantiated relative to Number of JStacks");

      // Verify each individual mock was used exactly once
      for (Shell.ShellCommandExecutor mockExecutor : mockedConstruction.constructed()) {
        verify(mockExecutor, times(1)).execute();
        verify(mockExecutor, times(1)).getOutput();
      }

      assertTrue(result.contains("--- JStack iteration 0"));
      assertTrue(result.contains("--- JStack iteration 1"));
      assertTrue(result.contains("--- JStack iteration 2"));
      assertTrue(result.contains(DUMMY_JSTACK));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCollectApplicationThreadDumpSuccess() {
    List<Long> pids = List.of(23L, 12L, 531L);

    Application mockApp = mock(Application.class);
    NM_CONTEXT.getApplications().put(APPLICATION_ID, mockApp);

    when(MOCK_ACLS_MANAGER.checkAccess(any(), any(), any(), any())).thenReturn(true);

    Map<ContainerId, List<Long>> containerPids = Map.of(CONTAINER_ID, pids);
    doReturn(containerPids).when(DIAGNOSTIC_JSTACK_SERVICE).getApplicationContainerPids(mockApp);

    ProcessHandle mockProcessHandle = mock(ProcessHandle.class);
    ProcessHandle.Info mockPhInfo = mock(ProcessHandle.Info.class);

    when(mockProcessHandle.info()).thenReturn(mockPhInfo);
    when(mockPhInfo.user()).thenReturn(Optional.empty());

    try(MockedStatic<ProcessHandle> mockedStaticProcess = mockStatic(ProcessHandle.class);
        MockedConstruction<Shell.ShellCommandExecutor> mockedConstruction =
            mockConstruction(Shell.ShellCommandExecutor.class,
              (mock, context) -> when(mock.getOutput()).thenReturn(DUMMY_JSTACK))
            // Wrap mockedStatic & mockedConstruction here to automatically close them
        ){
      mockedStaticProcess
        .when(() -> ProcessHandle.of(anyLong())).thenReturn(Optional.of(mockProcessHandle));

      HttpServletRequest mockRequest = mock(HttpServletRequest.class);
      String result = DIAGNOSTIC_JSTACK_SERVICE
          .collectApplicationThreadDump(APPLICATION_ID_STR, NUMBER_OF_JSTACKS, mockRequest);

      assertEquals(pids.size()*NUMBER_OF_JSTACKS, mockedConstruction.constructed().size(),
          "ShellCommandExecutor should be instantiated for each PID time Number Of JStacks");

      // Verify each individual mock was used exactly once
      for (Shell.ShellCommandExecutor mockExecutor : mockedConstruction.constructed()) {
        verify(mockExecutor, times(1)).execute();
        verify(mockExecutor, times(1)).getOutput();
      }

      assertTrue(result.contains("--- JStack iteration 0 for PID: 23 ---"));
      assertTrue(result.contains("--- JStack iteration 0 for PID: 12 ---"));
      assertTrue(result.contains("--- JStack iteration 0 for PID: 531 ---"));
      assertTrue(result.contains(DUMMY_JSTACK));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    NM_CONTEXT.getApplications().remove(APPLICATION_ID);
    // Clean up to avoid side effects on another test

  }


  @Test
  public void testCollectApplicationThreadDumpWhenProcessIdNotAlive() throws IOException {
    int numJStacks = 3;
    Application mockApp = mock(Application.class);
    NM_CONTEXT.getApplications().put(APPLICATION_ID, mockApp);

    when(MOCK_ACLS_MANAGER.checkAccess(any(), any(), any(), any())).thenReturn(true);

    Map<ContainerId, List<Long>> fakeContainerPids = Map.of(CONTAINER_ID, List.of(23L));

    doReturn(fakeContainerPids).when(DIAGNOSTIC_JSTACK_SERVICE)
        .getApplicationContainerPids(mockApp);

    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    String result = DIAGNOSTIC_JSTACK_SERVICE
        .collectApplicationThreadDump(APPLICATION_ID_STR, numJStacks, mockRequest);

    assertNotNull(result);
    assertTrue(result.contains("Thread Dumps for ContainerId: " + CONTAINER_ID_STR),
        "Output should contain the container ID");
    assertTrue(result.contains("Status: Skipped Process with PID"),
        "Since we don't mock ProcessHandle.of to return non empty it considers this PID is dead");

    NM_CONTEXT.getApplications().remove(APPLICATION_ID);
    // Clean up to avoid side effects on another test

  }



}
