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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.XmlCustomResourceTypeTestCase.toXml;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlBoolean;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlInt;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlLong;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Performs value verifications on
 * {@link org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo}
 * objects against the values of {@link ResourceRequest}. With the help of the
 * {@link Builder}, users can also make verifications of the custom resource
 * types and its values.
 */
public class ResourceRequestsXmlVerifications {
  private final ResourceRequest resourceRequest;
  private final Element requestInfo;
  private final Map<String, Long> customResourceTypes;
  private final List<String> expectedCustomResourceTypes;

  ResourceRequestsXmlVerifications(Builder builder) {
    this.resourceRequest = builder.resourceRequest;
    this.requestInfo = builder.requestInfo;
    this.customResourceTypes = builder.customResourceTypes;
    this.expectedCustomResourceTypes = builder.expectedCustomResourceTypes;
  }

  public static void verifyWithCustomResourceTypes(Element requestInfo,
      ResourceRequest resourceRequest, List<String> expectedResourceTypes) {

    createDefaultBuilder(requestInfo, resourceRequest)
        .withExpectedCustomResourceTypes(expectedResourceTypes)
        .withCustomResourceTypes(extractActualCustomResourceType(requestInfo,
            expectedResourceTypes))
        .build().verify();
  }

  private static Builder createDefaultBuilder(Element requestInfo,
      ResourceRequest resourceRequest) {
    return new ResourceRequestsXmlVerifications.Builder()
        .withRequest(resourceRequest).withRequestInfo(requestInfo);
  }

  private static Map<String, Long> extractActualCustomResourceType(
      Element requestInfo, List<String> expectedResourceTypes) {
    Element capability =
        (Element) requestInfo.getElementsByTagName("capability").item(0);

    return extractCustomResorceTypes(capability,
        Sets.newHashSet(expectedResourceTypes));
  }

  private static Map<String, Long> extractCustomResorceTypes(Element capability,
      Set<String> expectedResourceTypes) {
    assertEquals(
        toXml(capability) + " should have only one resourceInformations child!",
        1, capability.getElementsByTagName("resourceInformations").getLength());
    Element resourceInformations = (Element) capability
        .getElementsByTagName("resourceInformations").item(0);

    NodeList customResources =
        resourceInformations.getElementsByTagName("resourceInformation");

    // customResources will include vcores / memory as well
    assertEquals(
        "Different number of custom resource types found than expected",
        expectedResourceTypes.size(), customResources.getLength() - 2);

    Map<String, Long> resourceTypesAndValues = Maps.newHashMap();
    for (int i = 0; i < customResources.getLength(); i++) {
      Element customResource = (Element) customResources.item(i);
      String name = getXmlString(customResource, "name");
      String unit = getXmlString(customResource, "units");
      String resourceType = getXmlString(customResource, "resourceType");
      Long value = getXmlLong(customResource, "value");

      if (ResourceInformation.MEMORY_URI.equals(name)
          || ResourceInformation.VCORES_URI.equals(name)) {
        continue;
      }

      assertTrue("Custom resource type " + name + " not found",
          expectedResourceTypes.contains(name));
      assertEquals("k", unit);
      assertEquals(ResourceTypes.COUNTABLE,
          ResourceTypes.valueOf(resourceType));
      assertNotNull("Resource value should not be null for resource type "
          + resourceType + ", listing xml contents: " + toXml(customResource),
          value);
      resourceTypesAndValues.put(name, value);
    }

    return resourceTypesAndValues;
  }

  private void verify() {
    assertEquals("nodeLabelExpression doesn't match",
        resourceRequest.getNodeLabelExpression(),
        getXmlString(requestInfo, "nodeLabelExpression"));
    assertEquals("numContainers doesn't match",
        resourceRequest.getNumContainers(),
        getXmlInt(requestInfo, "numContainers"));
    assertEquals("relaxLocality doesn't match",
        resourceRequest.getRelaxLocality(),
        getXmlBoolean(requestInfo, "relaxLocality"));
    assertEquals("priority does not match",
        resourceRequest.getPriority().getPriority(),
        getXmlInt(requestInfo, "priority"));
    assertEquals("resourceName does not match",
        resourceRequest.getResourceName(),
        getXmlString(requestInfo, "resourceName"));
    Element capability = (Element) requestInfo
            .getElementsByTagName("capability").item(0);
    assertEquals("memory does not match",
        resourceRequest.getCapability().getMemorySize(),
        getXmlLong(capability, "memory"));
    assertEquals("vCores does not match",
        resourceRequest.getCapability().getVirtualCores(),
        getXmlLong(capability, "vCores"));

    for (String expectedCustomResourceType : expectedCustomResourceTypes) {
      assertTrue(
          "Custom resource type " + expectedCustomResourceType
              + " cannot be found!",
          customResourceTypes.containsKey(expectedCustomResourceType));

      Long resourceValue = customResourceTypes.get(expectedCustomResourceType);
      assertNotNull("Resource value should not be null!", resourceValue);
    }

    Element executionTypeRequest = (Element) requestInfo
        .getElementsByTagName("executionTypeRequest").item(0);
    assertEquals("executionType does not match",
        resourceRequest.getExecutionTypeRequest().getExecutionType().name(),
        getXmlString(executionTypeRequest, "executionType"));
    assertEquals("enforceExecutionType does not match",
        resourceRequest.getExecutionTypeRequest().getEnforceExecutionType(),
        getXmlBoolean(executionTypeRequest, "enforceExecutionType"));
  }

  /**
   * Builder class for {@link ResourceRequestsXmlVerifications}.
   */
  public static final class Builder {
    private List<String> expectedCustomResourceTypes = Lists.newArrayList();
    private Map<String, Long> customResourceTypes;
    private ResourceRequest resourceRequest;
    private Element requestInfo;

    Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    Builder withExpectedCustomResourceTypes(
        List<String> expectedCustomResourceTypes) {
      this.expectedCustomResourceTypes = expectedCustomResourceTypes;
      return this;
    }

    Builder withCustomResourceTypes(Map<String, Long> customResourceTypes) {
      this.customResourceTypes = customResourceTypes;
      return this;
    }

    Builder withRequest(ResourceRequest resourceRequest) {
      this.resourceRequest = resourceRequest;
      return this;
    }

    Builder withRequestInfo(Element requestInfo) {
      this.requestInfo = requestInfo;
      return this;
    }

    public ResourceRequestsXmlVerifications build() {
      return new ResourceRequestsXmlVerifications(this);
    }
  }
}
