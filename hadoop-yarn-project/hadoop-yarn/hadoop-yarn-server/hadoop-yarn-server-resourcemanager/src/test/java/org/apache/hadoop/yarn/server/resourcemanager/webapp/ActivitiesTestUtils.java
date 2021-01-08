/*
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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.GenericDiagnosticsCollector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Some Utils for activities tests.
 */
public final class ActivitiesTestUtils {

  public static final String TOTAL_RESOURCE_INSUFFICIENT_DIAGNOSTIC_PREFIX =
      ActivityDiagnosticConstant.NODE_TOTAL_RESOURCE_INSUFFICIENT_FOR_REQUEST
          + ", " + GenericDiagnosticsCollector.RESOURCE_DIAGNOSTICS_PREFIX;

  public static final String UNMATCHED_PARTITION_OR_PC_DIAGNOSTIC_PREFIX =
      ActivityDiagnosticConstant.
          NODE_DO_NOT_MATCH_PARTITION_OR_PLACEMENT_CONSTRAINTS + ", "
          + GenericDiagnosticsCollector.PLACEMENT_CONSTRAINT_DIAGNOSTICS_PREFIX;

  /*
   * Field names in response of scheduler/app activities.
   */
  public static final String FN_ACT_ALLOCATIONS = "allocations";

  public static final String FN_ACT_DIAGNOSTIC = "diagnostic";

  public static final String FN_ACT_ALLOCATION_STATE = "allocationState";

  public static final String FN_ACT_FINAL_ALLOCATION_STATE =
      "finalAllocationState";

  public static final String FN_ACT_NODE_ID = "nodeId";

  public static final String FN_ACT_NODE_IDS = "nodeIds";

  public static final String FN_ACT_COUNT = "count";

  public static final String FN_ACT_APP_PRIORITY = "appPriority";

  public static final String FN_ACT_REQUEST_PRIORITY = "requestPriority";

  public static final String FN_ACT_ALLOCATION_REQUEST_ID =
      "allocationRequestId";

  public static final String FN_APP_ACT_CHILDREN = "children";

  public static final String FN_APP_ACT_ROOT = "appActivities";

  public static final String FN_SCHEDULER_ACT_NAME = "name";

  public static final String FN_SCHEDULER_ACT_ALLOCATIONS_ROOT = "root";

  public static final String FN_SCHEDULER_ACT_CHILDREN = "children";

  public static final String FN_SCHEDULER_ACT_ROOT = "activities";

  private ActivitiesTestUtils(){}

  public static List<JSONObject> findInAllocations(JSONObject allocationObj,
      Predicate p) throws JSONException {
    List<JSONObject> target = new ArrayList<>();
    recursiveFindObj(allocationObj.getJSONObject(
        FN_SCHEDULER_ACT_ALLOCATIONS_ROOT), p,
        target);
    return target;
  }

  private static void recursiveFindObj(JSONObject obj, Predicate p,
      List<JSONObject> target) throws JSONException {
    if (p.test(obj)) {
      target.add(obj);
    }
    if (obj.has(FN_SCHEDULER_ACT_CHILDREN)) {
      JSONArray childrenObjs = obj.optJSONArray(FN_SCHEDULER_ACT_CHILDREN);
      if (childrenObjs != null) {
        for (int i = 0; i < childrenObjs.length(); i++) {
          recursiveFindObj(childrenObjs.getJSONObject(i), p, target);
        }
      } else {
        JSONObject childrenObj = obj.optJSONObject(FN_SCHEDULER_ACT_CHILDREN);
        recursiveFindObj(childrenObj, p, target);
      }
    }
  }

  public static SchedulingRequest schedulingRequest(int numContainers,
      int priority, long allocReqId, int cores, int mem,
      PlacementConstraint placementConstraintExpression, String... tags) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .allocationTags(new HashSet<>(Arrays.asList(tags))).executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true))
        .resourceSizing(ResourceSizing
            .newInstance(numContainers, Resource.newInstance(mem, cores)))
        .placementConstraintExpression(placementConstraintExpression).build();
  }


  public static void verifyNumberOfNodes(JSONObject allocation, int expectValue)
      throws Exception {
    if (allocation.isNull(FN_SCHEDULER_ACT_ALLOCATIONS_ROOT)) {
      assertEquals("State of allocation is wrong", expectValue, 0);
    } else {
      assertEquals("State of allocation is wrong", expectValue,
          1 + getNumberOfNodes(
              allocation.getJSONObject(FN_SCHEDULER_ACT_ALLOCATIONS_ROOT)));
    }
  }

  public static int getNumberOfNodes(JSONObject allocation) throws Exception {
    if (!allocation.isNull(FN_SCHEDULER_ACT_CHILDREN)) {
      Object object = allocation.get(FN_SCHEDULER_ACT_CHILDREN);
      if (object.getClass() == JSONObject.class) {
        return 1 + getNumberOfNodes((JSONObject) object);
      } else {
        int count = 0;
        for (int i = 0; i < ((JSONArray) object).length(); i++) {
          count += (1 + getNumberOfNodes(
              ((JSONArray) object).getJSONObject(i)));
        }
        return count;
      }
    } else {
      return 0;
    }
  }

  public static void verifyStateOfAllocations(JSONObject allocation,
      String nameToCheck, String expectState) throws Exception {
    assertEquals("State of allocation is wrong", expectState,
        allocation.get(nameToCheck));
  }

  public static void verifyNumberOfAllocations(JSONObject json, int expectValue)
      throws Exception {
    JSONObject activitiesJson;
    if (json.has(FN_APP_ACT_ROOT)) {
      activitiesJson = json.getJSONObject(FN_APP_ACT_ROOT);
    } else if (json.has(FN_SCHEDULER_ACT_ROOT)) {
      activitiesJson = json.getJSONObject(FN_SCHEDULER_ACT_ROOT);
    } else {
      throw new IllegalArgumentException("Can't parse allocations!");
    }
    if (activitiesJson.isNull(FN_ACT_ALLOCATIONS)) {
      assertEquals("Number of allocations is wrong", expectValue, 0);
    } else {
      Object object = activitiesJson.get(FN_ACT_ALLOCATIONS);
      if (object.getClass() == JSONObject.class) {
        assertEquals("Number of allocations is wrong", expectValue, 1);
      } else if (object.getClass() == JSONArray.class) {
        assertEquals("Number of allocations is wrong in: " + object,
            expectValue, ((JSONArray) object).length());
      }
    }
  }

  public static void verifyQueueOrder(JSONObject json, String expectOrder)
      throws Exception {
    String order = "";
    if (!json.isNull(FN_SCHEDULER_ACT_ALLOCATIONS_ROOT)) {
      JSONObject root = json.getJSONObject(FN_SCHEDULER_ACT_ALLOCATIONS_ROOT);
      order = root.getString(FN_SCHEDULER_ACT_NAME) + "-" + getQueueOrder(root);
    }
    assertEquals("Order of queue is wrong", expectOrder,
        order.substring(0, order.length() - 1));
  }

  public static String getQueueOrder(JSONObject node) throws Exception {
    if (!node.isNull(FN_SCHEDULER_ACT_CHILDREN)) {
      Object children = node.get(FN_SCHEDULER_ACT_CHILDREN);
      if (children.getClass() == JSONObject.class) {
        if (!((JSONObject) children).isNull(FN_ACT_APP_PRIORITY)) {
          return "";
        }
        return ((JSONObject) children).getString(FN_SCHEDULER_ACT_NAME) + "-"
            + getQueueOrder((JSONObject) children);
      } else if (children.getClass() == JSONArray.class) {
        String order = "";
        for (int i = 0; i < ((JSONArray) children).length(); i++) {
          JSONObject child = (JSONObject) ((JSONArray) children).get(i);
          if (!child.isNull(FN_ACT_APP_PRIORITY)) {
            return "";
          }
          order += (child.getString(FN_SCHEDULER_ACT_NAME) + "-"
              + getQueueOrder(child));
        }
        return order;
      }
    }
    return "";
  }

  public static JSONObject getFirstSubNodeFromJson(JSONObject json,
      String... hierarchicalFieldNames) {
    return getSubNodesFromJson(json, hierarchicalFieldNames).get(0);
  }

  public static List<JSONObject> getSubNodesFromJson(JSONObject json,
      String... hierarchicalFieldNames) {
    List<JSONObject> results = Lists.newArrayList(json);
    for (String fieldName : hierarchicalFieldNames) {
      results = results.stream().filter(e -> e.has(fieldName))
          .flatMap(e -> getJSONObjects(e, fieldName).stream())
          .collect(Collectors.toList());
      if (results.isEmpty()) {
        throw new IllegalArgumentException("Can't find hierarchical fields "
            + Arrays.toString(hierarchicalFieldNames));
      }
    }
    return results;
  }

  private static List<JSONObject> getJSONObjects(JSONObject json,
      String fieldName) {
    List<JSONObject> objects = new ArrayList<>();
    if (json.has(fieldName)) {
      try {
        Object tmpObj = json.get(fieldName);
        if (tmpObj.getClass() == JSONObject.class) {
          objects.add((JSONObject) tmpObj);
        } else if (tmpObj.getClass() == JSONArray.class) {
          for (int i = 0; i < ((JSONArray) tmpObj).length(); i++) {
            objects.add(((JSONArray) tmpObj).getJSONObject(i));
          }
        }
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }
    return objects;
  }

  public static void verifyNumberOfAllocationAttempts(JSONObject allocation,
      int expectValue) throws Exception {
    if (allocation.isNull(FN_APP_ACT_CHILDREN)) {
      assertEquals("Number of allocation attempts is wrong", expectValue, 0);
    } else {
      Object object = allocation.get(FN_APP_ACT_CHILDREN);
      if (object.getClass() == JSONObject.class) {
        assertEquals("Number of allocations attempts is wrong", expectValue, 1);
      } else if (object.getClass() == JSONArray.class) {
        assertEquals("Number of allocations attempts is wrong", expectValue,
            ((JSONArray) object).length());
      }
    }
  }

  public static JSONObject requestWebResource(WebResource webResource,
      MultivaluedMap<String, String> params) {
    if (params != null) {
      webResource = webResource.queryParams(params);
    }
    ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    return response.getEntity(JSONObject.class);
  }

  /**
   * Convert format using {name} (HTTP base) into %s (Java based).
   * @param format Initial format using {}.
   * @param args Arguments for the format.
   * @return New format using %s.
   */
  public static String format(String format, Object... args) {
    Pattern p = Pattern.compile("\\{.*?}");
    Matcher m = p.matcher(format);
    String newFormat = m.replaceAll("%s");
    return String.format(newFormat, args);
  }
}
