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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMemoryApplicationHistoryStore extends
    ApplicationHistoryStoreTestUtils {

  @BeforeEach
  public void setup() {
    store = new MemoryApplicationHistoryStore();
  }

  @Test
  void testReadWriteApplicationHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    try {
      writeApplicationFinishData(appId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "is stored before the start information"));
    }
    // Normal
    int numApps = 5;
    for (int i = 1; i <= numApps; ++i) {
      appId = ApplicationId.newInstance(0, i);
      writeApplicationStartData(appId);
      writeApplicationFinishData(appId);
    }
    assertEquals(numApps, store.getAllApplications().size());
    for (int i = 1; i <= numApps; ++i) {
      appId = ApplicationId.newInstance(0, i);
      ApplicationHistoryData data = store.getApplication(appId);
      assertNotNull(data);
      assertEquals(appId.toString(), data.getApplicationName());
      assertEquals(appId.toString(), data.getDiagnosticsInfo());
    }
    // Write again
    appId = ApplicationId.newInstance(0, 1);
    try {
      writeApplicationStartData(appId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeApplicationFinishData(appId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @Test
  void testReadWriteApplicationAttemptHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "is stored before the start information"));
    }
    // Normal
    int numAppAttempts = 5;
    writeApplicationStartData(appId);
    for (int i = 1; i <= numAppAttempts; ++i) {
      appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      writeApplicationAttemptStartData(appAttemptId);
      writeApplicationAttemptFinishData(appAttemptId);
    }
    assertEquals(numAppAttempts, store.getApplicationAttempts(appId)
        .size());
    for (int i = 1; i <= numAppAttempts; ++i) {
      appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      ApplicationAttemptHistoryData data =
          store.getApplicationAttempt(appAttemptId);
      assertNotNull(data);
      assertEquals(appAttemptId.toString(), data.getHost());
      assertEquals(appAttemptId.toString(), data.getDiagnosticsInfo());
    }
    writeApplicationFinishData(appId);
    // Write again
    appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptStartData(appAttemptId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  void testReadWriteContainerHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerFinishData(containerId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "is stored before the start information"));
    }
    // Normal
    writeApplicationAttemptStartData(appAttemptId);
    int numContainers = 5;
    for (int i = 1; i <= numContainers; ++i) {
      containerId = ContainerId.newContainerId(appAttemptId, i);
      writeContainerStartData(containerId);
      writeContainerFinishData(containerId);
    }
    assertEquals(numContainers, store.getContainers(appAttemptId).size());
    for (int i = 1; i <= numContainers; ++i) {
      containerId = ContainerId.newContainerId(appAttemptId, i);
      ContainerHistoryData data = store.getContainer(containerId);
      assertNotNull(data);
      assertEquals(Priority.newInstance(containerId.getId()),
          data.getPriority());
      assertEquals(containerId.toString(), data.getDiagnosticsInfo());
    }
    ContainerHistoryData masterContainer = store.getAMContainer(appAttemptId);
    assertNotNull(masterContainer);
    assertEquals(ContainerId.newContainerId(appAttemptId, 1),
        masterContainer.getContainerId());
    writeApplicationAttemptFinishData(appAttemptId);
    // Write again
    containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerStartData(containerId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeContainerFinishData(containerId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @Test
  void testMassiveWriteContainerHistory() throws IOException {
    long mb = 1024 * 1024;
    Runtime runtime = Runtime.getRuntime();
    long usedMemoryBefore = (runtime.totalMemory() - runtime.freeMemory()) / mb;
    int numContainers = 100000;
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    for (int i = 1; i <= numContainers; ++i) {
      ContainerId containerId = ContainerId.newContainerId(appAttemptId, i);
      writeContainerStartData(containerId);
      writeContainerFinishData(containerId);
    }
    long usedMemoryAfter = (runtime.totalMemory() - runtime.freeMemory()) / mb;
    assertTrue((usedMemoryAfter - usedMemoryBefore) < 400);
  }

}
