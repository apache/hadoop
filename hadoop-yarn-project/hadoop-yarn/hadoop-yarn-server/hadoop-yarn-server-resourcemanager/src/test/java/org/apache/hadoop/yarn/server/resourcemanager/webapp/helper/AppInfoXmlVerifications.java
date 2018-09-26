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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    assertEquals("priority doesn't match", 0, getXmlInt(info, "priority"));
    checkStringMatch("state", app.getState().toString(),
            getXmlString(info, "state"));
    checkStringMatch("finalStatus", app
            .getFinalApplicationStatus().toString(),
            getXmlString(info, "finalStatus"));
    assertEquals("progress doesn't match", 0, getXmlFloat(info, "progress"),
        0.0);
    if ("UNASSIGNED".equals(getXmlString(info, "trackingUI"))) {
      checkStringMatch("trackingUI", "UNASSIGNED",
              getXmlString(info, "trackingUI"));
    }
    WebServicesTestUtils.checkStringEqual("diagnostics",
            app.getDiagnostics().toString(), getXmlString(info, "diagnostics"));
    assertEquals("clusterId doesn't match",
            ResourceManager.getClusterTimeStamp(),
            getXmlLong(info, "clusterId"));
    assertEquals("startedTime doesn't match", app.getStartTime(),
            getXmlLong(info, "startedTime"));
    assertEquals("finishedTime doesn't match", app.getFinishTime(),
            getXmlLong(info, "finishedTime"));
    assertTrue("elapsed time not greater than 0",
            getXmlLong(info, "elapsedTime") > 0);
    checkStringMatch("amHostHttpAddress", app
                    .getCurrentAppAttempt().getMasterContainer()
                    .getNodeHttpAddress(),
            getXmlString(info, "amHostHttpAddress"));
    assertTrue("amContainerLogs doesn't match",
        getXmlString(info, "amContainerLogs").startsWith("http://"));
    assertTrue("amContainerLogs doesn't contain user info",
        getXmlString(info, "amContainerLogs").endsWith("/" + app.getUser()));
    assertEquals("allocatedMB doesn't match", 1024,
            getXmlInt(info, "allocatedMB"));
    assertEquals("allocatedVCores doesn't match", 1,
            getXmlInt(info, "allocatedVCores"));
    assertEquals("queueUsagePerc doesn't match", 50.0f,
            getXmlFloat(info, "queueUsagePercentage"), 0.01f);
    assertEquals("clusterUsagePerc doesn't match", 50.0f,
            getXmlFloat(info, "clusterUsagePercentage"), 0.01f);
    assertEquals("numContainers doesn't match", 1,
        getXmlInt(info, "runningContainers"));
    assertNotNull("preemptedResourceSecondsMap should not be null",
            info.getElementsByTagName("preemptedResourceSecondsMap"));
    assertEquals("preemptedResourceMB doesn't match", app
                    .getRMAppMetrics().getResourcePreempted().getMemorySize(),
            getXmlInt(info, "preemptedResourceMB"));
    assertEquals("preemptedResourceVCores doesn't match", app
                    .getRMAppMetrics().getResourcePreempted().getVirtualCores(),
            getXmlInt(info, "preemptedResourceVCores"));
    assertEquals("numNonAMContainerPreempted doesn't match", app
                    .getRMAppMetrics().getNumNonAMContainersPreempted(),
            getXmlInt(info, "numNonAMContainerPreempted"));
    assertEquals("numAMContainerPreempted doesn't match", app
                    .getRMAppMetrics().getNumAMContainersPreempted(),
            getXmlInt(info, "numAMContainerPreempted"));
    assertEquals("Log aggregation Status doesn't match", app
                    .getLogAggregationStatusForAppReport().toString(),
            getXmlString(info, "logAggregationStatus"));
    assertEquals("unmanagedApplication doesn't match", app
                    .getApplicationSubmissionContext().getUnmanagedAM(),
            getXmlBoolean(info, "unmanagedApplication"));
    assertEquals("unmanagedApplication doesn't match",
            app.getApplicationSubmissionContext().getNodeLabelExpression(),
            getXmlString(info, "appNodeLabelExpression"));
    assertEquals("unmanagedApplication doesn't match",
            app.getAMResourceRequests().get(0).getNodeLabelExpression(),
            getXmlString(info, "amNodeLabelExpression"));
    assertEquals("amRPCAddress",
            AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()),
            getXmlString(info, "amRPCAddress"));
  }
}
