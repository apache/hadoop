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
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.w3c.dom.Element;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringMatch;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlBoolean;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlFloat;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlInt;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlLong;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contains all value verifications that are needed to verify {@link AppInfo}
 * XML documents.
 */
public final class AppInfoXmlVerifications {

  private AppInfoXmlVerifications() {
    //utility class
  }

  /**
   * Tests whether {@link AppInfo} representation object contains the required
   * values as per defined in the specified app parameter.
   * @param info
   * @param  app  an RMApp instance that contains the required values
   */
  public static void verify(Element info, RMApp app) {
    checkStringMatch("id", app.getApplicationId()
            .toString(), getXmlString(info, "id"));
    checkStringMatch("user", app.getUser(),
            getXmlString(info, "user"));
    checkStringMatch("name", app.getName(),
            getXmlString(info, "name"));
    checkStringMatch("applicationType",
            app.getApplicationType(), getXmlString(info, "applicationType"));
    checkStringMatch("queue", app.getQueue(),
            getXmlString(info, "queue"));
    assertEquals(0, getXmlInt(info, "priority"), "priority doesn't match");
    checkStringMatch("state", app.getState().toString(),
            getXmlString(info, "state"));
    checkStringMatch("finalStatus", app
            .getFinalApplicationStatus().toString(),
            getXmlString(info, "finalStatus"));
    assertEquals(0, getXmlFloat(info, "progress"),
        0.0, "progress doesn't match");
    if ("UNASSIGNED".equals(getXmlString(info, "trackingUI"))) {
      checkStringMatch("trackingUI", "UNASSIGNED",
              getXmlString(info, "trackingUI"));
    }
    WebServicesTestUtils.checkStringEqual("diagnostics",
            app.getDiagnostics().toString(), getXmlString(info, "diagnostics"));
    assertEquals(ResourceManager.getClusterTimeStamp(),
        getXmlLong(info, "clusterId"), "clusterId doesn't match");
    assertEquals(app.getStartTime(),
        getXmlLong(info, "startedTime"), "startedTime doesn't match");
    assertEquals(app.getFinishTime(),
        getXmlLong(info, "finishedTime"), "finishedTime doesn't match");
    assertTrue(getXmlLong(info, "elapsedTime") > 0,
        "elapsed time not greater than 0");
    checkStringMatch("amHostHttpAddress", app
                    .getCurrentAppAttempt().getMasterContainer()
                    .getNodeHttpAddress(),
            getXmlString(info, "amHostHttpAddress"));
    assertTrue(getXmlString(info, "amContainerLogs").startsWith("http://"),
        "amContainerLogs doesn't match");
    assertTrue(getXmlString(info, "amContainerLogs").endsWith("/" + app.getUser()),
        "amContainerLogs doesn't contain user info");
    assertEquals(1024, getXmlInt(info, "allocatedMB"), "allocatedMB doesn't match");
    assertEquals(1, getXmlInt(info, "allocatedVCores"),
        "allocatedVCores doesn't match");
    assertEquals(50.0f, getXmlFloat(info, "queueUsagePercentage"), 0.01f,
        "queueUsagePerc doesn't match");
    assertEquals(50.0f, getXmlFloat(info, "clusterUsagePercentage"), 0.01f,
        "clusterUsagePerc doesn't match");
    assertEquals(1, getXmlInt(info, "runningContainers"),
        "numContainers doesn't match");
    assertNotNull(info.getElementsByTagName("preemptedResourceSecondsMap"),
        "preemptedResourceSecondsMap should not be null");
    assertEquals(app.getRMAppMetrics().getResourcePreempted().getMemorySize(),
        getXmlInt(info, "preemptedResourceMB"), "preemptedResourceMB doesn't match");
    assertEquals(app.getRMAppMetrics().getResourcePreempted().getVirtualCores(),
        getXmlInt(info, "preemptedResourceVCores"), "preemptedResourceVCores doesn't match");
    assertEquals(app.getRMAppMetrics().getNumNonAMContainersPreempted(),
        getXmlInt(info, "numNonAMContainerPreempted"), "numNonAMContainerPreempted doesn't match");
    assertEquals(app.getRMAppMetrics().getNumAMContainersPreempted(),
        getXmlInt(info, "numAMContainerPreempted"),
        "numAMContainerPreempted doesn't match");
    assertEquals(app.getLogAggregationStatusForAppReport().toString(),
        getXmlString(info, "logAggregationStatus"),
        "Log aggregation Status doesn't match");
    assertEquals(app.getApplicationSubmissionContext().getUnmanagedAM(),
        getXmlBoolean(info, "unmanagedApplication"),
        "unmanagedApplication doesn't match");
    assertEquals(app.getApplicationSubmissionContext().getNodeLabelExpression(),
        getXmlString(info, "appNodeLabelExpression"),
        "unmanagedApplication doesn't match");
    assertEquals(app.getAMResourceRequests().get(0).getNodeLabelExpression(),
        getXmlString(info, "amNodeLabelExpression"),
        "unmanagedApplication doesn't match");
    assertEquals(AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()),
        getXmlString(info, "amRPCAddress"), "amRPCAddress");
  }
}
