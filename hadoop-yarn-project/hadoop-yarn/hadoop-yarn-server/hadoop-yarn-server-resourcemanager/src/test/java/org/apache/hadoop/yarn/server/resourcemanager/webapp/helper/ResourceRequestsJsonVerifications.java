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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Performs value verifications on
 * {@link org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo}
 * objects against the values of {@link ResourceRequest}. With the help of the
 * {@link Builder}, users can also make verifications of the custom resource
 * types and its values.
 */
public class ResourceRequestsJsonVerifications {
  private final ResourceRequest resourceRequest;
  private final JSONObject requestInfo;
  private final Map<String, Long> customResourceTypes;
  private final List<String> expectedCustomResourceTypes;

  ResourceRequestsJsonVerifications(Builder builder) {
    this.resourceRequest = builder.resourceRequest;
    this.requestInfo = builder.requestInfo;
    this.customResourceTypes = builder.customResourceTypes;
    this.expectedCustomResourceTypes = builder.expectedCustomResourceTypes;
  }

  public static void verify(JSONObject requestInfo, ResourceRequest rr)
      throws JSONException {
    createDefaultBuilder(requestInfo, rr).build().verify();
  }

  public static void verifyWithCustomResourceTypes(JSONObject requestInfo,
      ResourceRequest resourceRequest, List<String> expectedResourceTypes)
      throws JSONException {

    createDefaultBuilder(requestInfo, resourceRequest)
        .withExpectedCustomResourceTypes(expectedResourceTypes)
        .withCustomResourceTypes(
            extractActualCustomResourceTypes(requestInfo, expectedResourceTypes))
        .build().verify();
  }

  private static Builder createDefaultBuilder(JSONObject requestInfo,
      ResourceRequest resourceRequest) {
    return new ResourceRequestsJsonVerifications.Builder()
            .withRequest(resourceRequest)
            .withRequestInfoJson(requestInfo);
  }

  private static Map<String, Long> extractActualCustomResourceTypes(
      JSONObject requestInfo, List<String> expectedResourceTypes)
      throws JSONException {
    JSONObject capability = requestInfo.getJSONObject("capability");
    Map<String, Long> resourceAndValue =
        extractCustomResorceTypeValues(capability, expectedResourceTypes);
    Map.Entry<String, Long> resourceEntry =
        resourceAndValue.entrySet().iterator().next();

    assertTrue(
        "Found resource type: " + resourceEntry.getKey()
            + " is not in expected resource types: " + expectedResourceTypes,
        expectedResourceTypes.contains(resourceEntry.getKey()));

    return resourceAndValue;
  }

  private static Map<String, Long> extractCustomResorceTypeValues(
      JSONObject capability, List<String> expectedResourceTypes)
      throws JSONException {
    assertTrue(
        "resourceCategory does not have resourceInformations: " + capability,
        capability.has("resourceInformations"));

    JSONObject resourceInformations =
        capability.getJSONObject("resourceInformations");
    assertTrue(
        "resourceInformations does not have resourceInformation object: "
            + resourceInformations,
        resourceInformations.has("resourceInformation"));
    JSONArray customResources =
        resourceInformations.getJSONArray("resourceInformation");

    // customResources will include vcores / memory as well
    assertEquals(
        "Different number of custom resource types found than expected",
        expectedResourceTypes.size(), customResources.length() - 2);

    Map<String, Long> resourceValues = Maps.newHashMap();
    for (int i = 0; i < customResources.length(); i++) {
      JSONObject customResource = customResources.getJSONObject(i);
      assertTrue("Resource type does not have name field: " + customResource,
          customResource.has("name"));
      assertTrue("Resource type does not have name resourceType field: "
          + customResource, customResource.has("resourceType"));
      assertTrue(
          "Resource type does not have name units field: " + customResource,
          customResource.has("units"));
      assertTrue(
          "Resource type does not have name value field: " + customResource,
          customResource.has("value"));

      String name = customResource.getString("name");
      String unit = customResource.getString("units");
      String resourceType = customResource.getString("resourceType");
      Long value = customResource.getLong("value");

      if (ResourceInformation.MEMORY_URI.equals(name)
          || ResourceInformation.VCORES_URI.equals(name)) {
        continue;
      }

      assertTrue("Custom resource type " + name + " not found",
          expectedResourceTypes.contains(name));
      assertEquals("k", unit);
      assertEquals(ResourceTypes.COUNTABLE,
          ResourceTypes.valueOf(resourceType));
      assertNotNull("Custom resource value " + value + " is null!", value);
      resourceValues.put(name, value);
    }

    return resourceValues;
  }

  private void verify() throws JSONException {
    assertEquals("nodeLabelExpression doesn't match",
        resourceRequest.getNodeLabelExpression(),
            requestInfo.getString("nodeLabelExpression"));
    assertEquals("numContainers doesn't match",
            resourceRequest.getNumContainers(),
            requestInfo.getInt("numContainers"));
    assertEquals("relaxLocality doesn't match",
            resourceRequest.getRelaxLocality(),
            requestInfo.getBoolean("relaxLocality"));
    assertEquals("priority does not match",
            resourceRequest.getPriority().getPriority(),
            requestInfo.getInt("priority"));
    assertEquals("resourceName does not match",
            resourceRequest.getResourceName(),
            requestInfo.getString("resourceName"));
    assertEquals("memory does not match",
        resourceRequest.getCapability().getMemorySize(),
            requestInfo.getJSONObject("capability").getLong("memory"));
    assertEquals("vCores does not match",
        resourceRequest.getCapability().getVirtualCores(),
            requestInfo.getJSONObject("capability").getLong("vCores"));

    verifyAtLeastOneCustomResourceIsSerialized();

    JSONObject executionTypeRequest =
            requestInfo.getJSONObject("executionTypeRequest");
    assertEquals("executionType does not match",
        resourceRequest.getExecutionTypeRequest().getExecutionType().name(),
            executionTypeRequest.getString("executionType"));
    assertEquals("enforceExecutionType does not match",
            resourceRequest.getExecutionTypeRequest().getEnforceExecutionType(),
            executionTypeRequest.getBoolean("enforceExecutionType"));
  }

  /**
   * JSON serialization produces "invalid JSON" by default as maps are
   * serialized like this:
   * "customResources":{"entry":{"key":"customResource-1","value":"0"}}
   * If the map has multiple keys then multiple entries will be serialized.
   * Our json parser in tests cannot handle duplicates therefore only one
   * custom resource will be in the parsed json. See:
   * https://issues.apache.org/jira/browse/YARN-7505
   */
  private void verifyAtLeastOneCustomResourceIsSerialized() {
    boolean resourceFound = false;
    for (String expectedCustomResourceType : expectedCustomResourceTypes) {
      if (customResourceTypes.containsKey(expectedCustomResourceType)) {
        resourceFound = true;
        Long resourceValue =
            customResourceTypes.get(expectedCustomResourceType);
        assertNotNull("Resource value should not be null!", resourceValue);
      }
    }
    assertTrue("No custom resource type can be found in the response!",
        resourceFound);
  }

  /**
   * Builder class for {@link ResourceRequestsJsonVerifications}.
   */
  public static final class Builder {
    private List<String> expectedCustomResourceTypes = Lists.newArrayList();
    private Map<String, Long> customResourceTypes;
    private ResourceRequest resourceRequest;
    private JSONObject requestInfo;

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

    Builder withCustomResourceTypes(
            Map<String, Long> customResourceTypes) {
      this.customResourceTypes = customResourceTypes;
      return this;
    }

    Builder withRequest(ResourceRequest resourceRequest) {
      this.resourceRequest = resourceRequest;
      return this;
    }

    Builder withRequestInfoJson(JSONObject requestInfo) {
      this.requestInfo = requestInfo;
      return this;
    }

    public ResourceRequestsJsonVerifications build() {
      return new ResourceRequestsJsonVerifications(this);
    }
  }
}
