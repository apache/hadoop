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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
        .AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler
        .CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.AppInfoJsonVerifications;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.AppInfoXmlVerifications;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.ResourceRequestsJsonVerifications;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.ResourceRequestsXmlVerifications;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.junit.Assert.assertEquals;

public class TestRMWebServicesCustomResourceTypesCommons {

  static void verifyAppsXML(NodeList appArray, RMApp app, ResourceManager rm) {
    for (int i = 0; i < appArray.getLength(); i++) {
      Element element = (Element) appArray.item(i);
      AppInfoXmlVerifications.verify(element, app);

      NodeList resourceRequests =
          element.getElementsByTagName("resourceRequests");
      assertEquals(1, resourceRequests.getLength());
      Node resourceRequest = resourceRequests.item(0);
      ResourceRequest rr =
          ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
              .getApplicationAttempt(
                  app.getCurrentAppAttempt().getAppAttemptId())
              .getAppSchedulingInfo().getAllResourceRequests().get(0);
      ResourceRequestsXmlVerifications.verifyWithCustomResourceTypes(
          (Element) resourceRequest, rr,
          CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
    }
  }

  static void verifyAppInfoJson(JSONObject info, RMApp app, ResourceManager rm)
      throws JSONException {
    int expectedNumberOfElements = getExpectedNumberOfElements(app);

    assertEquals("incorrect number of elements", expectedNumberOfElements,
        info.length());

    AppInfoJsonVerifications.verify(info, app);

    JSONArray resourceRequests = info.getJSONArray("resourceRequests");
    JSONObject requestInfo = resourceRequests.getJSONObject(0);
    ResourceRequest rr =
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getApplicationAttempt(app.getCurrentAppAttempt().getAppAttemptId())
            .getAppSchedulingInfo().getAllResourceRequests().get(0);

    ResourceRequestsJsonVerifications.verifyWithCustomResourceTypes(requestInfo,
        rr, CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  static int getExpectedNumberOfElements(RMApp app) {
    int expectedNumberOfElements = 40 + 2; // 2 -> resourceRequests
    if (app.getApplicationSubmissionContext()
        .getNodeLabelExpression() != null) {
      expectedNumberOfElements++;
    }

    if (app.getAMResourceRequests().get(0).getNodeLabelExpression() != null) {
      expectedNumberOfElements++;
    }

    if (AppInfo
        .getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()) != null) {
      expectedNumberOfElements++;
    }
    return expectedNumberOfElements;
  }
}
