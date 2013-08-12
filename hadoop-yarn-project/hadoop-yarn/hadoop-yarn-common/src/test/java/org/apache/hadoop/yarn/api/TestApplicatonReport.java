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

package org.apache.hadoop.yarn.api;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestApplicatonReport {

  @Test
  public void testApplicationReport() {
    long timestamp = System.currentTimeMillis();
    ApplicationReport appReport1 =
        createApplicationReport(1, 1, timestamp);
    ApplicationReport appReport2 =
        createApplicationReport(1, 1, timestamp);
    ApplicationReport appReport3 =
        createApplicationReport(1, 1, timestamp);
    Assert.assertEquals(appReport1, appReport2);
    Assert.assertEquals(appReport2, appReport3);
    appReport1.setApplicationId(null);
    Assert.assertNull(appReport1.getApplicationId());
    Assert.assertNotSame(appReport1, appReport2);
    appReport2.setCurrentApplicationAttemptId(null);
    Assert.assertNull(appReport2.getCurrentApplicationAttemptId());
    Assert.assertNotSame(appReport2, appReport3);
    Assert.assertNull(appReport1.getAMRMToken());
  }

  protected static ApplicationReport createApplicationReport(
      int appIdInt, int appAttemptIdInt, long timestamp) {
    ApplicationId appId = ApplicationId.newInstance(timestamp, appIdInt);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, appAttemptIdInt);
    ApplicationReport appReport =
        ApplicationReport.newInstance(appId, appAttemptId, "user", "queue",
          "appname", "host", 124, null, YarnApplicationState.FINISHED,
          "diagnostics", "url", 0, 0, FinalApplicationStatus.SUCCEEDED, null,
          "N/A", 0.53789f, YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
    return appReport;
  }

}

