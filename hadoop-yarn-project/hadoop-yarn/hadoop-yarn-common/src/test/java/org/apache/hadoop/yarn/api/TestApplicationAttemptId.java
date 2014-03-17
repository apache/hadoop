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

import org.junit.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

public class TestApplicationAttemptId {

  @Test
  public void testApplicationAttemptId() {
    ApplicationAttemptId a1 = createAppAttemptId(10l, 1, 1);
    ApplicationAttemptId a2 = createAppAttemptId(10l, 1, 2);
    ApplicationAttemptId a3 = createAppAttemptId(10l, 2, 1);
    ApplicationAttemptId a4 = createAppAttemptId(8l, 1, 4);
    ApplicationAttemptId a5 = createAppAttemptId(10l, 1, 1);
    
    Assert.assertTrue(a1.equals(a5));
    Assert.assertFalse(a1.equals(a2));
    Assert.assertFalse(a1.equals(a3));
    Assert.assertFalse(a1.equals(a4));
    
    Assert.assertTrue(a1.compareTo(a5) == 0);
    Assert.assertTrue(a1.compareTo(a2) < 0);
    Assert.assertTrue(a1.compareTo(a3) < 0);
    Assert.assertTrue(a1.compareTo(a4) > 0);
    
    Assert.assertTrue(a1.hashCode() == a5.hashCode());
    Assert.assertFalse(a1.hashCode() == a2.hashCode());
    Assert.assertFalse(a1.hashCode() == a3.hashCode());
    Assert.assertFalse(a1.hashCode() == a4.hashCode());
    
    long ts = System.currentTimeMillis();
    ApplicationAttemptId a6 = createAppAttemptId(ts, 543627, 33492611);
    Assert.assertEquals("appattempt_10_0001_000001", a1.toString());
    Assert.assertEquals("appattempt_" + ts + "_543627_33492611", a6.toString());
  }

  private ApplicationAttemptId createAppAttemptId(
      long clusterTimeStamp, int id, int attemptId) {
    ApplicationId appId = ApplicationId.newInstance(clusterTimeStamp, id);
    return ApplicationAttemptId.newInstance(appId, attemptId);
  }

  public static void main(String[] args) throws Exception {
    TestApplicationAttemptId t = new TestApplicationAttemptId();
    t.testApplicationAttemptId();
  }
}
