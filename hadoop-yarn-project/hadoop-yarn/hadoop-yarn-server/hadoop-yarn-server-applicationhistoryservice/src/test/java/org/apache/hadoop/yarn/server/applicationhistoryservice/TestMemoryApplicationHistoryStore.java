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

import org.junit.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryApplicationHistoryStore extends
    ApplicationHistoryStoreTestUtils {

  @Before
  public void setup() {
    store = new MemoryApplicationHistoryStore();
  }

  @Test
  public void testReadWriteApplicationHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    try {
      writeApplicationFinishData(appId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(
        "is stored before the start information"));
    }
    // Normal
    int numApps = 5;
    for (int i = 1; i <= numApps; ++i) {
      appId = ApplicationId.newInstance(0, i);
      writeApplicationStartData(appId);
      writeApplicationFinishData(appId);
    }
    Assert.assertEquals(numApps, store.getAllApplications().size());
    for (int i = 1; i <= numApps; ++i) {
      appId = ApplicationId.newInstance(0, i);
      ApplicationHistoryData data = store.getApplication(appId);
      Assert.assertNotNull(data);
      Assert.assertEquals(appId.toString(), data.getApplicationName());
      Assert.assertEquals(appId.toString(), data.getDiagnosticsInfo());
    }
    // Write again
    appId = ApplicationId.newInstance(0, 1);
    try {
      writeApplicationStartData(appId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeApplicationFinishData(appId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @Test
  public void testReadWriteApplicationAttemptHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(
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
    Assert.assertEquals(numAppAttempts, store.getApplicationAttempts(appId)
      .size());
    for (int i = 1; i <= numAppAttempts; ++i) {
      appAttemptId = ApplicationAttemptId.newInstance(appId, i);
      ApplicationAttemptHistoryData data =
          store.getApplicationAttempt(appAttemptId);
      Assert.assertNotNull(data);
      Assert.assertEquals(appAttemptId.toString(), data.getHost());
      Assert.assertEquals(appAttemptId.toString(), data.getDiagnosticsInfo());
    }
    writeApplicationFinishData(appId);
    // Write again
    appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptStartData(appAttemptId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testReadWriteContainerHistory() throws Exception {
    // Out of order
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerFinishData(containerId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(
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
    Assert
      .assertEquals(numContainers, store.getContainers(appAttemptId).size());
    for (int i = 1; i <= numContainers; ++i) {
      containerId = ContainerId.newContainerId(appAttemptId, i);
      ContainerHistoryData data = store.getContainer(containerId);
      Assert.assertNotNull(data);
      Assert.assertEquals(Priority.newInstance(containerId.getId()),
        data.getPriority());
      Assert.assertEquals(containerId.toString(), data.getDiagnosticsInfo());
    }
    ContainerHistoryData masterContainer = store.getAMContainer(appAttemptId);
    Assert.assertNotNull(masterContainer);
    Assert.assertEquals(ContainerId.newContainerId(appAttemptId, 1),
      masterContainer.getContainerId());
    writeApplicationAttemptFinishData(appAttemptId);
    // Write again
    containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerStartData(containerId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
    try {
      writeContainerFinishData(containerId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is already stored"));
    }
  }

  @Test
  public void testMassiveWriteContainerHistory() throws IOException {
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
    Assert.assertTrue((usedMemoryAfter - usedMemoryBefore) < 400);
  }

}
