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
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationAttemptHistoryDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationHistoryDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ContainerHistoryDataPBImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryApplicationHistoryStore {
  MemoryApplicationHistoryStore memstore = null;

  @Before
  public void setup() throws Throwable {
    memstore = MemoryApplicationHistoryStore.getMemoryStore();
    writeHistoryApplication();
    writeHistoryApplicationAttempt();
    writeContainer();
  }

  public void writeHistoryApplication() throws Throwable {
    ApplicationHistoryData appData = new ApplicationHistoryDataPBImpl();
    appData.setApplicationId(ApplicationId.newInstance(1234, 1));
    memstore.writeApplication(appData);
  }

  public void writeHistoryApplicationAttempt() throws Throwable {
    ApplicationAttemptHistoryData appAttemptHistoryData =
        new ApplicationAttemptHistoryDataPBImpl();
    appAttemptHistoryData.setApplicationAttemptId(ApplicationAttemptId
      .newInstance(ApplicationId.newInstance(1234, 1), 1));
    memstore.writeApplicationAttempt(appAttemptHistoryData);
  }

  public void writeContainer() throws Throwable {
    ContainerHistoryData container = new ContainerHistoryDataPBImpl();
    container.setContainerId(ContainerId.newInstance(ApplicationAttemptId
      .newInstance(ApplicationId.newInstance(1234, 1), 1), 1));
    memstore.writeContainer(container);
  }

  public ContainerHistoryData writeContainer(ApplicationAttemptId appAttemptId,
      int containerId) throws Throwable {
    ContainerHistoryData container = new ContainerHistoryDataPBImpl();
    container
      .setContainerId(ContainerId.newInstance(appAttemptId, containerId));
    memstore.writeContainer(container);
    return container;
  }
  
  @After
  public void tearDown() {
  }

  @Test
  public void testReadApplication() throws Throwable {
    HashMap<ApplicationId, ApplicationHistoryData> map =
        (HashMap<ApplicationId, ApplicationHistoryData>) memstore
          .getAllApplications();
    Assert.assertEquals(1, map.size());
    ApplicationHistoryData appData = null;
    for (ApplicationId appId : map.keySet()) {
      appData = map.get(appId);
      Assert.assertEquals("application_1234_0001", appData.getApplicationId()
        .toString());
    }
    HashMap<ApplicationAttemptId, ApplicationAttemptHistoryData> appAttempts =
        (HashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>) memstore
          .getApplicationAttempts(appData.getApplicationId());
    Assert.assertEquals(1, appAttempts.size());
    ApplicationAttemptHistoryData appAttempt = null;
    for (ApplicationAttemptId appAttemptId : appAttempts.keySet()) {
      appAttempt = appAttempts.get(appAttemptId);
      Assert.assertEquals("appattempt_1234_0001_000001", appAttempt
        .getApplicationAttemptId().toString());
    }
    ContainerHistoryData amContainer =
        memstore.getContainer(ContainerId.newInstance(ApplicationAttemptId
          .newInstance(ApplicationId.newInstance(1234, 1), 1), 1));
    Assert.assertEquals("container_1234_0001_01_000001", amContainer
      .getContainerId().toString());
    ContainerHistoryData container2 =
        writeContainer(appAttempt.getApplicationAttemptId(), 2);
    HashMap<ContainerId, ContainerHistoryData> containers =
        (HashMap<ContainerId, ContainerHistoryData>) memstore
          .getContainers(appAttempt.getApplicationAttemptId());
    Assert.assertEquals(2, containers.size());
    Assert.assertEquals("container_1234_0001_01_000002", containers.get(
      container2.getContainerId()).getContainerId().toString());
  }
}
