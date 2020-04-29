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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationHistoryManagerImpl extends
    ApplicationHistoryStoreTestUtils {
  ApplicationHistoryManagerImpl applicationHistoryManagerImpl = null;

  @Before
  public void setup() throws Exception {
    Configuration config = new Configuration();
    config.setClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
      MemoryApplicationHistoryStore.class, ApplicationHistoryStore.class);
    applicationHistoryManagerImpl = new ApplicationHistoryManagerImpl();
    applicationHistoryManagerImpl.init(config);
    applicationHistoryManagerImpl.start();
    store = applicationHistoryManagerImpl.getHistoryStore();
  }

  @After
  public void tearDown() throws Exception {
    applicationHistoryManagerImpl.stop();
  }

  @Test
  public void testApplicationReport() throws IOException, YarnException {
    ApplicationId appId = null;
    appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    writeApplicationFinishData(appId);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    writeApplicationAttemptStartData(appAttemptId);
    writeApplicationAttemptFinishData(appAttemptId);
    ApplicationReport appReport =
        applicationHistoryManagerImpl.getApplication(appId);
    Assert.assertNotNull(appReport);
    Assert.assertEquals(appId, appReport.getApplicationId());
    Assert.assertEquals(appAttemptId,
      appReport.getCurrentApplicationAttemptId());
    Assert.assertEquals(appAttemptId.toString(), appReport.getHost());
    Assert.assertEquals("test type", appReport.getApplicationType().toString());
    Assert.assertEquals("test queue", appReport.getQueue().toString());
  }

  @Test
  public void testApplications() throws IOException {
    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    ApplicationId appId2 = ApplicationId.newInstance(0, 2);
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    writeApplicationStartData(appId1, 1000);
    writeApplicationFinishData(appId1);
    writeApplicationStartData(appId2, 3000);
    writeApplicationFinishData(appId2);
    writeApplicationStartData(appId3, 4000);
    writeApplicationFinishData(appId3);
    Map<ApplicationId, ApplicationReport> reports =
        applicationHistoryManagerImpl.getApplications(2, 2000L, 5000L);
    Assert.assertNotNull(reports);
    Assert.assertEquals(2, reports.size());
    Assert.assertNull(reports.get("1"));
    Assert.assertNull(reports.get("2"));
    Assert.assertNull(reports.get("3"));
  }
}
