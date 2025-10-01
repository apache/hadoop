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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.helper;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringEqual;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(0, info.getInt("priority"), "priority doesn't match");
    checkStringMatch("state", app.getState().toString(),
        info.getString("state"));
    checkStringMatch("finalStatus", app.getFinalApplicationStatus().toString(),
        info.getString("finalStatus"));
    assertEquals(0, (float) info.getDouble("progress"), 0.0,
        "progress doesn't match");
    if ("UNASSIGNED".equals(info.getString("trackingUI"))) {
      checkStringMatch("trackingUI", "UNASSIGNED",
          info.getString("trackingUI"));
    }
    checkStringEqual("diagnostics", app.getDiagnostics().toString(),
        info.getString("diagnostics"));
    assertEquals(ResourceManager.getClusterTimeStamp(), info.getLong("clusterId"),
        "clusterId doesn't match");
    assertEquals(app.getStartTime(), info.getLong("startedTime"),
        "startedTime doesn't match");
    assertEquals(app.getFinishTime(), info.getLong("finishedTime"),
        "finishedTime doesn't match");
    assertTrue(info.getLong("elapsedTime") > 0, "elapsed time not greater than 0");
    checkStringMatch("amHostHttpAddress",
        app.getCurrentAppAttempt().getMasterContainer().getNodeHttpAddress(),
        info.getString("amHostHttpAddress"));
    assertTrue(info.getString("amContainerLogs").startsWith("http://"),
        "amContainerLogs doesn't match");
    assertTrue(info.getString("amContainerLogs").endsWith("/" + app.getUser()),
        "amContainerLogs doesn't contain user info");
    assertEquals(1024, info.getInt("allocatedMB"), "allocatedMB doesn't match");
    assertEquals(1, info.getInt("allocatedVCores"), "allocatedVCores doesn't match");
    assertEquals(50.0f, (float) info.getDouble("queueUsagePercentage"), 0.01f,
        "queueUsagePerc doesn't match");
    assertEquals(50.0f, (float) info.getDouble("clusterUsagePercentage"), 0.01f,
        "clusterUsagePerc doesn't match");
    assertEquals(1, info.getInt("runningContainers"),
        "numContainers doesn't match");
    assertNotNull(info.get("preemptedResourceSecondsMap"),
        "preemptedResourceSecondsMap should not be null");
    assertEquals(app.getRMAppMetrics().getResourcePreempted().getMemorySize(),
        info.getInt("preemptedResourceMB"), "preemptedResourceMB doesn't match");
    assertEquals(app.getRMAppMetrics().getResourcePreempted().getVirtualCores(),
        info.getInt("preemptedResourceVCores"),
        "preemptedResourceVCores doesn't match");
    assertEquals(app.getRMAppMetrics().getNumNonAMContainersPreempted(),
        info.getInt("numNonAMContainerPreempted"),
        "numNonAMContainerPreempted doesn't match");
    assertEquals(app.getRMAppMetrics().getNumAMContainersPreempted(),
        info.getInt("numAMContainerPreempted"),
        "numAMContainerPreempted doesn't match");
    assertEquals(app.getLogAggregationStatusForAppReport().toString(),
        info.getString("logAggregationStatus"),
        "Log aggregation Status doesn't match");
    assertEquals(app.getApplicationSubmissionContext().getUnmanagedAM(),
        info.getBoolean("unmanagedApplication"),
        "unmanagedApplication doesn't match");

    if (app.getApplicationSubmissionContext()
        .getNodeLabelExpression() != null) {
      assertEquals(app.getApplicationSubmissionContext().getNodeLabelExpression(),
          info.getString("appNodeLabelExpression"),
          "appNodeLabelExpression doesn't match");
    }
    assertEquals(app.getAMResourceRequests().get(0).getNodeLabelExpression(),
        info.getString("amNodeLabelExpression"),
        "amNodeLabelExpression doesn't match");
    assertEquals(AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()),
        info.getString("amRPCAddress"), "amRPCAddress");
  }
}
