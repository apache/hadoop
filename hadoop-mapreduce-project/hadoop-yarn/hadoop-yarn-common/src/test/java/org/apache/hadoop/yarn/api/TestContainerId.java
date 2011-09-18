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

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestContainerId {

  @Test
  public void testContainerId() {
    ContainerId c1 = createContainerId(10l, 1, 1, 1);
    ContainerId c2 = createContainerId(10l, 1, 1, 2);
    ContainerId c3 = createContainerId(10l, 1, 1, 1);
    ContainerId c4 = createContainerId(10l, 1, 3, 1);
    ContainerId c5 = createContainerId(8l, 1, 3, 1);

    Assert.assertTrue(c1.equals(c3));
    Assert.assertFalse(c1.equals(c2));
    Assert.assertFalse(c1.equals(c4));
    Assert.assertFalse(c1.equals(c5));

    Assert.assertTrue(c1.compareTo(c3) == 0);
    Assert.assertTrue(c1.compareTo(c2) < 0);
    Assert.assertTrue(c1.compareTo(c4) < 0);
    Assert.assertTrue(c1.compareTo(c5) > 0);

    Assert.assertTrue(c1.hashCode() == c3.hashCode());
    Assert.assertFalse(c1.hashCode() == c2.hashCode());
    Assert.assertFalse(c1.hashCode() == c4.hashCode());
    Assert.assertFalse(c1.hashCode() == c5.hashCode());
    
    long ts = System.currentTimeMillis();
    ContainerId c6 = createContainerId(ts, 36473, 4365472, 25645811);
    Assert.assertEquals("container_10_0001_01_000001", c1.toString());
    Assert.assertEquals("container_" + ts + "_36473_4365472_25645811",
        c6.toString());
  }

  private ContainerId createContainerId(long clusterTimestamp, int appIdInt,
      int appAttemptIdInt, int containerIdInt) {
    ApplicationId appId = createAppId(clusterTimestamp, appIdInt);
    ApplicationAttemptId appAttemptId =
        createAppAttemptId(appId, appAttemptIdInt);
    ContainerId containerId = Records.newRecord(ContainerId.class);
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(containerIdInt);
    return containerId;
  }

  private ApplicationId createAppId(long clusterTimeStamp, int id) {
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(clusterTimeStamp);
    appId.setId(id);
    return appId;
  }

  private ApplicationAttemptId createAppAttemptId(ApplicationId appId,
      int attemptId) {
    ApplicationAttemptId appAttemptId =
        Records.newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(attemptId);
    return appAttemptId;
  }
}
