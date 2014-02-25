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
}
