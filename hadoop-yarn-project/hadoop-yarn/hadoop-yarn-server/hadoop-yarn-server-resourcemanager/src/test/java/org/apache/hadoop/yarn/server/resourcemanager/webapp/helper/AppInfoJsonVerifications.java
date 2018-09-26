/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.helper;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringEqual;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringMatch;
import static org.junit.Assert.*;

/**
 * Contains all value verifications that are needed to verify {@link AppInfo}
 * JSON objects.
 */
public final class AppInfoJsonVerifications {

  private AppInfoJsonVerifications() {
    //utility class
  }

  /**
   * Tests whether {@link AppInfo} representation object contains the required
   * values as per defined in the specified app parameter.
   * @param  app  an RMApp instance that contains the required values
   *              to test against.
   */
  public static void verify(JSONObject info, RMApp app) throws JSONException {
    checkStringMatch("id", app.getApplicationId().toString(),
        info.getString("id"));
    checkStringMatch("user", app.getUser(), info.getString("user"));
    checkStringMatch("name", app.getName(), info.getString("name"));
    checkStringMatch("applicationType", app.getApplicationType(),
        info.getString("applicationType"));
    checkStringMatch("queue", app.getQueue(), info.getString("queue"));
    assertEquals("priority doesn't match", 0, info.getInt("priority"));
    checkStringMatch("state", app.getState().toString(),
        info.getString("state"));
    checkStringMatch("finalStatus", app.getFinalApplicationStatus().toString(),
        info.getString("finalStatus"));
    assertEquals("progress doesn't match", 0,
        (float) info.getDouble("progress"), 0.0);
    if ("UNASSIGNED".equals(info.getString("trackingUI"))) {
      checkStringMatch("trackingUI", "UNASSIGNED",
          info.getString("trackingUI"));
    }
    checkStringEqual("diagnostics", app.getDiagnostics().toString(),
        info.getString("diagnostics"));
    assertEquals("clusterId doesn't match",
        ResourceManager.getClusterTimeStamp(), info.getLong("clusterId"));
    assertEquals("startedTime doesn't match", app.getStartTime(),
        info.getLong("startedTime"));
    assertEquals("finishedTime doesn't match", app.getFinishTime(),
        info.getLong("finishedTime"));
    assertTrue("elapsed time not greater than 0",
        info.getLong("elapsedTime") > 0);
    checkStringMatch("amHostHttpAddress",
        app.getCurrentAppAttempt().getMasterContainer().getNodeHttpAddress(),
        info.getString("amHostHttpAddress"));
    assertTrue("amContainerLogs doesn't match",
        info.getString("amContainerLogs").startsWith("http://"));
    assertTrue("amContainerLogs doesn't contain user info",
        info.getString("amContainerLogs").endsWith("/" + app.getUser()));
    assertEquals("allocatedMB doesn't match", 1024, info.getInt("allocatedMB"));
    assertEquals("allocatedVCores doesn't match", 1,
        info.getInt("allocatedVCores"));
    assertEquals("queueUsagePerc doesn't match", 50.0f,
        (float) info.getDouble("queueUsagePercentage"), 0.01f);
    assertEquals("clusterUsagePerc doesn't match", 50.0f,
        (float) info.getDouble("clusterUsagePercentage"), 0.01f);
    assertEquals("numContainers doesn't match", 1,
        info.getInt("runningContainers"));
    assertNotNull("preemptedResourceSecondsMap should not be null",
        info.getJSONObject("preemptedResourceSecondsMap"));
    assertEquals("preemptedResourceMB doesn't match",
        app.getRMAppMetrics().getResourcePreempted().getMemorySize(),
        info.getInt("preemptedResourceMB"));
    assertEquals("preemptedResourceVCores doesn't match",
        app.getRMAppMetrics().getResourcePreempted().getVirtualCores(),
        info.getInt("preemptedResourceVCores"));
    assertEquals("numNonAMContainerPreempted doesn't match",
        app.getRMAppMetrics().getNumNonAMContainersPreempted(),
        info.getInt("numNonAMContainerPreempted"));
    assertEquals("numAMContainerPreempted doesn't match",
        app.getRMAppMetrics().getNumAMContainersPreempted(),
        info.getInt("numAMContainerPreempted"));
    assertEquals("Log aggregation Status doesn't match",
        app.getLogAggregationStatusForAppReport().toString(),
        info.getString("logAggregationStatus"));
    assertEquals("unmanagedApplication doesn't match",
        app.getApplicationSubmissionContext().getUnmanagedAM(),
        info.getBoolean("unmanagedApplication"));

    if (app.getApplicationSubmissionContext()
        .getNodeLabelExpression() != null) {
      assertEquals("appNodeLabelExpression doesn't match",
          app.getApplicationSubmissionContext().getNodeLabelExpression(),
          info.getString("appNodeLabelExpression"));
    }
    assertEquals("amNodeLabelExpression doesn't match",
        app.getAMResourceRequests().get(0).getNodeLabelExpression(),
        info.getString("amNodeLabelExpression"));
    assertEquals("amRPCAddress",
        AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()),
        info.getString("amRPCAddress"));
  }
}
