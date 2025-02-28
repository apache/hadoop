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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;

/**
 * RequestHandler is used to handle requests from applications,
 * It handles requests at the beginning of CapacityScheduler#allocate,
 * and manages multiple update items which define which requests
 * should be chosen and how to update them. based on the capacity-scheduler
 * configuration and can be updated dynamically without restarting the RM.
 */
public class RequestsHandler {

  protected static final Logger LOG =
      LoggerFactory.getLogger(RequestsHandler.class);

  private static final Pattern PLACEHOLDER_PATTERN =
      Pattern.compile("\\$\\{[^}]+\\}");

  private static final String APP_INFO_KEY_QUEUE = "queue";
  private static final String APP_INFO_KEY_USER = "user";
  private static final String APP_INFO_KEY_PRIORITY = "priority";
  private static final String APP_INFO_KEY_ID = "id";
  private static final String APP_INFO_KEY_NAME = "name";
  private static final String APP_INFO_KEY_TYPE = "type";
  private static final String APP_INFO_KEY_TAGS = "tags";
  private static final String REQUEST_INFO_KEY_PRIORITY = "priority";
  private static final String REQUEST_INFO_KEY_RESOURCE_NAME = "resourceName";
  private static final String REQUEST_INFO_KEY_RELAX_LOCALITY = "relaxLocality";
  private static final String REQUEST_INFO_KEY_EXECUTION_TYPE = "executionType";
  private static final String REQUEST_INFO_KEY_ALLOCATION_TAGS =
      "allocationTags";
  private static final String REQUEST_INFO_KEY_IS_AM = "isAM";

  private static final Set<String> VALID_PLACEHOLDER_KEYS =
      Sets.newHashSet(APP_INFO_KEY_QUEUE, APP_INFO_KEY_USER,
          APP_INFO_KEY_PRIORITY, APP_INFO_KEY_ID, APP_INFO_KEY_NAME,
          APP_INFO_KEY_TYPE, APP_INFO_KEY_TAGS);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final ScriptEngine SCRIPT_ENGINE =
      new ScriptEngineManager().getEngineByName("JavaScript");

  private final Function<ApplicationAttemptId, Pair<FiCaSchedulerApp, RMApp>>
      appProvider;
  private final ReentrantReadWriteLock.WriteLock writeLock;
  private final ReentrantReadWriteLock.ReadLock readLock;

  private boolean enabled = false;

  private List<UpdateItem> updateItems;

  // current updates conf value for comparing
  private String updatesConfV;

  public RequestsHandler(Function<ApplicationAttemptId,
          Pair<FiCaSchedulerApp, RMApp>> appProvider) {
    this.appProvider = appProvider;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    writeLock = lock.writeLock();
    readLock = lock.readLock();
  }

  public void initialize(Configuration conf)
      throws IOException, YarnException {
    if (SCRIPT_ENGINE == null) {
      // disabled if script engine is not found
      LOG.warn("Disabled RequestsHandler since script engine not found");
      return;
    }
    boolean newEnabled =
        conf.getBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
            CapacitySchedulerConfiguration.DEFAULT_REQUEST_HANDLER_ENABLED);
    List<UpdateItem> newUpdateItems = null;
    String newUpdatesConfV = null;
    if (newEnabled) {
      newUpdatesConfV =
          conf.get(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES);
      UpdatesConf newUpdatesConf = null;
      if (newUpdatesConfV != null && !newUpdatesConfV.isEmpty()) {
        newUpdatesConf =
            OBJECT_MAPPER.readValue(newUpdatesConfV, UpdatesConf.class);
      }
      if (newUpdatesConf != null && newUpdatesConf.getItems() != null &&
          !newUpdatesConf.getItems().isEmpty()) {
        newUpdateItems = new ArrayList<>();
        for (int i = 0; i < newUpdatesConf.getItems().size(); i++) {
          UpdateItemConf updateItemConf = newUpdatesConf.getItems().get(i);
          newUpdateItems.add(new UpdateItem(i, updateItemConf, this.appProvider));
        }
      }
    }
    // update
    writeLock.lock();
    try{
      if (enabled == newEnabled &&
          StringUtils.equals(newUpdatesConfV, updatesConfV)) {
        LOG.info("No changes detected in RequestsHandler configuration," +
            " enabled={}, updatesConf={}", enabled, updatesConfV);
        return;
      }
      enabled = newEnabled;
      updateItems = newUpdateItems;
      updatesConfV = newUpdatesConfV;
      LOG.info("Initialized request updater, enabled={}, updatesConf={}",
          enabled, updatesConfV);
    } finally {
      writeLock.unlock();
    }
  }

  public RequestsHandleResponse handle(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests) {
    readLock.lock();
    try {
      if (!enabled || updateItems == null || updateItems.isEmpty()) {
        return null;
      }
      return updateRequests(appAttemptId, resourceRequests, schedulingRequests);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * UpdatesConf is the root object of the configuration.
   */
  public static class UpdatesConf {

    @JsonProperty("items")
    private List<UpdateItemConf> items;

    public List<UpdateItemConf> getItems() {
      return items;
    }

    public void setItems(List<UpdateItemConf> items) {
      this.items = items;
    }
  }

  public static class UpdateItemConf {

    @JsonProperty("appMatchExpr")
    private String appMatchExpr;

    @JsonProperty("requestMatchExpr")
    private String requestMatchExpr;

    // whether to convert ResourceRequest to SchedulingRequest
    @JsonProperty("isRRToSR")
    private boolean isRRToSR;

    @JsonProperty("partition")
    private String partition;

    @JsonProperty("executionType")
    private String executionType;

    @JsonProperty("allocationTags")
    private Set<String> allocationTags;

    @JsonProperty("placementConstraint")
    private String placementConstraint;

    public String getAppMatchExpr() {
      return appMatchExpr;
    }

    public void setAppMatchExpr(String appMatchExpr) {
      this.appMatchExpr = appMatchExpr;
    }

    public String getRequestMatchExpr() {
      return requestMatchExpr;
    }

    public void setRequestMatchExpr(String requestMatchExpr) {
      this.requestMatchExpr = requestMatchExpr;
    }

    public boolean isRRToSR() {
      return isRRToSR;
    }

    public void setIsRRToSR(boolean isRRToSR) {
      this.isRRToSR = isRRToSR;
    }

    public String getPartition() {
      return partition;
    }

    public void setPartition(String partition) {
      this.partition = partition;
    }

    public Set<String> getAllocationTags() {
      return allocationTags;
    }

    public void setAllocationTags(Set<String> allocationTags) {
      this.allocationTags = allocationTags;
    }

    public String getPlacementConstraint() {
      return placementConstraint;
    }

    public void setPlacementConstraint(String placementConstraint) {
      this.placementConstraint = placementConstraint;
    }

    public String getExecutionType() {
      return executionType;
    }

    public void setExecutionType(String executionType) {
      this.executionType = executionType;
    }

    public String toString() {
      return "{" +
          "appMatchExpr='" + appMatchExpr + '\'' +
          ", requestMatchExpr='" + requestMatchExpr + '\'' +
          ", isRRToSR=" + isRRToSR +
          ", partition='" + partition + '\'' +
          ", executionType='" + executionType + '\'' +
          ", allocationTags=" + allocationTags +
          ", placementConstraint='" + placementConstraint + '\'' +
          '}';
    }
  }

  public RequestsHandleResponse updateRequests(
      ApplicationAttemptId appAttemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests) {
    boolean isUpdated = false;
    // update requests
    for (UpdateItem updateItem : updateItems) {
      DynamicAppInfo dynamicAppInfo =
          updateItem.getDynamicAppInfo(appAttemptId);
      if (dynamicAppInfo == null) {
        break;
      }
      if (!dynamicAppInfo.isMatched) {
        continue;
      }

      RequestsHandleResponse resp =
          updateItem.updateRequests(appAttemptId.getApplicationId(),
              resourceRequests, schedulingRequests, dynamicAppInfo);
      if (resp.isUpdated()) {
        isUpdated = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Updated requests: appId={}, updateItemConf={}, RR={}, SR={}",
              appAttemptId.getApplicationId(), updateItem.updateItemConf.toString(),
              resp.getResourceRequests(), resp.getSchedulingRequests());
        }
      }
      resourceRequests = resp.getResourceRequests();
      schedulingRequests = resp.getSchedulingRequests();
    }
    return new RequestsHandleResponse(isUpdated, resourceRequests,
        schedulingRequests);
  }

  @VisibleForTesting
  public boolean isEnabled() {
    return enabled;
  }

  @VisibleForTesting
  public List<UpdateItem> getUpdateItems() {
    return updateItems;
  }

  /**
   * UpdateItem is responsible for applying configured request updates based on
   * matching rules.
   * Variable substitution allows using placeholders like ${queue}, ${user},
   * etc. in placement constraints and allocation tags.
   * These variables are replaced with actual application properties at runtime.
   */
  public static class UpdateItem {

    Function<ApplicationAttemptId, Pair<FiCaSchedulerApp, RMApp>> appProvider;

    private final int confIndex;

    /*
     * Precompiled or preprocessed fields designed to enhance the performance
     * of the update process.
     */
    private CompiledScript appMatchScript;
    private CompiledScript requestMatchScript;
    private PlacementConstraint placementConstraint;
    private ExecutionType executionType;
    private boolean hasPlaceholderForPC;
    private boolean hasPlaceholderForAllocTags;

    // Configuration for this update item
    private final UpdateItemConf updateItemConf;

    // Loader and Cache for dynamic app info
    private final CacheLoader<ApplicationAttemptId, Optional<DynamicAppInfo>>
        dynamicAppInfoLoader =
        new CacheLoader<ApplicationAttemptId, Optional<DynamicAppInfo>>() {
          @Override
          public Optional<DynamicAppInfo> load(ApplicationAttemptId appAttemptId) {
            Pair<FiCaSchedulerApp, RMApp> appPair =
                appProvider.apply(appAttemptId);
            if (appPair == null || appPair.getLeft() == null ||
                appPair.getRight() == null) {
              return Optional.empty();
            }
            FiCaSchedulerApp app = appPair.getLeft();
            RMApp rmApp = appPair.getRight();
            Map<String, Object> appInfo = convertToAppInfo(app, rmApp);
            Map<String, String> appStrInfo = appInfo.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> e.getValue() != null ? e.getValue().toString() : ""));
            boolean isMatched =
                isAppMatch(appAttemptId.getApplicationId(), appInfo);
            PlacementConstraint runtimePC =
                getRuntimePlacementConstraint(appStrInfo);
            Set<String> allocationTags = getRuntimeAllocationTags(appStrInfo);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Loaded dynamic app info: confIndex={}, appId={}, " +
                      "isMatched={}, placementConstraint={}, allocationTags={}",
                  confIndex, appAttemptId.getApplicationId(), isMatched,
                  runtimePC, allocationTags);
            }
            return Optional.of(new DynamicAppInfo(app, isMatched, runtimePC,
                allocationTags));
          }
        };

    private final LoadingCache<ApplicationAttemptId, Optional<DynamicAppInfo>>
        dynamicAppInfoCache =
        CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
            .build(dynamicAppInfoLoader);

    /**
     * Constructs an UpdateItem with the specified configuration.
     * Compiles scripts for application and request matching.
     * Parses execution types and placement constraints from configuration.
     *
     * @param confIndex the index of this update item in the configuration
     * @param updateItemConf the configuration for this update item
     * @param appProvider a function to provide application information
     * @throws YarnException if the scripts, execution type, or
     *                        placement constraint cannot be parsed
     */
    public UpdateItem(int confIndex, UpdateItemConf updateItemConf,
        Function<ApplicationAttemptId, Pair<FiCaSchedulerApp, RMApp>>
            appProvider)
        throws YarnException {
      this.confIndex = confIndex;
      this.appProvider = appProvider;
      // compile app/request match-scripts
      if (updateItemConf.getAppMatchExpr() != null) {
        try {
          appMatchScript = ((Compilable) SCRIPT_ENGINE).compile(
              updateItemConf.getAppMatchExpr());
        } catch (ScriptException e) {
          throw new YarnException("Failed to compile app match expression: "
              + updateItemConf.getAppMatchExpr(), e);
        }
      }
      if (updateItemConf.getRequestMatchExpr() != null) {
        try {
          requestMatchScript = ((Compilable) SCRIPT_ENGINE).compile(
              updateItemConf.getRequestMatchExpr());
        } catch (ScriptException e) {
          throw new YarnException("Failed to compile request match expression: "
              + updateItemConf.getRequestMatchExpr(), e);
        }
      }
      // parse execution type
      if (updateItemConf.getExecutionType() != null) {
        try{
          executionType = ExecutionType.valueOf(updateItemConf.getExecutionType());
        } catch (IllegalArgumentException e) {
          throw new YarnException("Failed to parse execution-type: " +
                  updateItemConf.getExecutionType(), e);
        }
      }
      // determine if placement constraint contains placeholders
      // and parse it if static
      if (updateItemConf.getPlacementConstraint() != null) {
        // parse placement constraint
        try {
          PlacementConstraint.AbstractConstraint absConstraint =
              PlacementConstraintParser.parseExpression(
                  updateItemConf.getPlacementConstraint());
          placementConstraint = new PlacementConstraint(absConstraint);
        } catch (PlacementConstraintParseException e) {
          throw new YarnException("Failed to parse placement-constraint: " +
              updateItemConf.getPlacementConstraint(), e);
        }
        // mark hasPlaceholder flag for placement constraint
        if (hasPlaceholder(updateItemConf.getPlacementConstraint())) {
          hasPlaceholderForPC = true;
        }
      }
      // mark hasPlaceholder flag for allocation tags
      if (updateItemConf.getAllocationTags() != null) {
        for (String tag : updateItemConf.getAllocationTags()) {
          if (hasPlaceholder(tag)) {
            hasPlaceholderForAllocTags = true;
          }
        }
      }

      // include updateItemConf
      this.updateItemConf = updateItemConf;
    }

    public DynamicAppInfo getDynamicAppInfo(ApplicationAttemptId appAttemptId) {
      try {
        Optional<DynamicAppInfo> opt = dynamicAppInfoCache.get(appAttemptId);
        if (opt.isPresent()) {
          return opt.get();
        }
      } catch (Exception e) {
        LOG.error("Failed to get dynamic app info for appId: {}",
            appAttemptId.getApplicationId(), e);
      }
      return null;
    }

    /**
     * Checks if an application matches this update item's criteria.
     * Uses JavaScript evaluation of the appMatchExpr against application properties.
     *
     * @param appId the application ID
     * @param appInfo map of application information
     * @return true if the application matches, false otherwise
     */
    public boolean isAppMatch(ApplicationId appId,
        Map<String, Object> appInfo) {
      if (appMatchScript == null) {
        return true;
      }
      try {
        Bindings bindings = SCRIPT_ENGINE.createBindings();
        bindings.putAll(appInfo);
        Boolean isMatched = (Boolean) appMatchScript.eval(bindings);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Check app: appId={}, isMatched={}, appInfo={}, appMatchExpr={}",
              appId, isMatched, appInfo, updateItemConf.getAppMatchExpr());
        }
        return isMatched;
      } catch (Exception e) {
        LOG.error(
            "Failed to evaluate app-match-expr: appId={}, appMatchExpr={}",
            appId, updateItemConf.getAppMatchExpr(), e);
        return false;
      }
    }

    /**
     * Checks if a request matches this update item's criteria.
     * Uses JavaScript evaluation of the requestMatchExpr against request properties.
     *
     * @param appId the application ID
     * @param infoSupplier supplier for request information
     * @return true if the request matches, false otherwise
     */
    public boolean isRequestMatch(ApplicationId appId,
        Supplier<Map<String, Object>> infoSupplier) {
      if (requestMatchScript == null) {
        return true;
      }
      Map<String, Object> info = infoSupplier.get();
      try {
        Bindings bindings = SCRIPT_ENGINE.createBindings();
        bindings.putAll(info);
        Boolean isMatched = (Boolean) requestMatchScript.eval(bindings);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Check request: appId={}, isMatched={}, reqInfo={}, requestMatchExpr={}",
              appId, isMatched, info, updateItemConf.getRequestMatchExpr());
        }
        return isMatched;
      } catch (Exception e) {
        LOG.error("Failed to evaluate request-filter-expression: {}",
            updateItemConf.getRequestMatchExpr(), e);
        return false;
      }
    }

    /**
     * Updates resource requests and/or scheduling requests based on this update
     * item's configuration. May convert resource requests to scheduling
     * requests if isRRToSR is configured. Applies updates to each matching
     * request, including execution type, placement constraints, and
     * allocation tags with variable substitution.
     *
     * @param appId the application ID
     * @param resourceRequests list of resource requests to process
     * @param schedulingRequests list of scheduling requests to process
     * @param dynamicAppInfo dynamic application information
     * @return response containing updated requests and update status
     */
    private RequestsHandleResponse updateRequests(
        ApplicationId appId,
        List<ResourceRequest> resourceRequests,
        List<SchedulingRequest> schedulingRequests,
        DynamicAppInfo dynamicAppInfo) {
      boolean isUpdated = false;
      // update resource requests
      if (resourceRequests != null) {
        for (ResourceRequest rr: resourceRequests) {
          if (!isRequestMatch(appId,
              () -> convertToRequestInfo(dynamicAppInfo, rr))) {
            continue;
          }
          updateResourceRequest(appId, rr);
          isUpdated = true;
        }
      }
      // when both isUpdated and isRRToSR are true, convert to SR at first
      if (resourceRequests != null && !resourceRequests.isEmpty() &&
          isUpdated && updateItemConf.isRRToSR) {
        schedulingRequests = resourceRequests.stream()
            .map(UpdateItem::convertToSchedulingRequest)
            .collect(Collectors.toList());
        resourceRequests = null;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Converted to scheduling requests: appId={}, sr={}",
              appId, schedulingRequests);
        }
      }
      // update scheduling requests
      if (schedulingRequests != null) {
        for (SchedulingRequest sr: schedulingRequests) {
          if (!isRequestMatch(appId, () -> convertToRequestInfo(dynamicAppInfo, sr))) {
            continue;
          }
          updateSchedulingRequest(appId, sr, dynamicAppInfo);
          isUpdated = true;
        }
      }
      return new RequestsHandleResponse(isUpdated, resourceRequests,
          schedulingRequests);
    }

    /**
     * Converts a ResourceRequest to a SchedulingRequest.
     * Preserves allocation request ID, priority, resource sizing, and node label expression.
     * Maps node label expressions to placement constraints.
     *
     * @param resourceRequest the resource request to convert
     * @return a new scheduling request with equivalent properties
     */
    public static SchedulingRequest convertToSchedulingRequest(
        ResourceRequest resourceRequest) {
      if (resourceRequest == null) {
        return SchedulingRequest.newBuilder().build();
      }
      // Compatible with Hadoop2.x
      // whose default value of execution-type-request is null
      ExecutionTypeRequest executionTypeRequest =
          resourceRequest.getExecutionTypeRequest();
      if (executionTypeRequest == null) {
        executionTypeRequest = ExecutionTypeRequest.newInstance();
      }
      SchedulingRequest sr = SchedulingRequest.newBuilder()
          .executionType(executionTypeRequest)
          .allocationRequestId(resourceRequest.getAllocationRequestId())
          .priority(resourceRequest.getPriority())
          .resourceSizing(ResourceSizing.newInstance(
              resourceRequest.getNumContainers(),
              resourceRequest.getCapability())).build();
      if (resourceRequest.getNodeLabelExpression() != null) {
        PlacementConstraint constraint =
            PlacementConstraints.targetNodeAttribute(NODE,
                NodeAttributeOpCode.EQ,
                PlacementConstraints.PlacementTargets.nodePartition(
                    resourceRequest.getNodeLabelExpression())).build();
        sr.setPlacementConstraint(constraint);
      }
      return sr;
    }

    /**
     * Updates a ResourceRequest with configuration from this update item.
     * Can modify node label expression (partition) and execution type.
     *
     * @param appId application ID for logging
     * @param rr resource request to update
     */
    private void updateResourceRequest(ApplicationId appId,
        ResourceRequest rr) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Before updating resource request, appId={}, RR={}, conf={}",
            appId, rr, updateItemConf.toString());
      }
      if (updateItemConf.partition != null) {
        rr.setNodeLabelExpression(updateItemConf.partition);
      }
      if (executionType != null) {
        rr.setExecutionTypeRequest(
            ExecutionTypeRequest.newInstance(executionType));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Done updating resource request, appId={}, RR={}", appId, rr);
      }
    }

    /**
     * Updates a SchedulingRequest with configuration from this update item.
     * Can modify execution type, placement constraint and allocation tags.
     *
     * @param appId application ID for logging
     * @param sr scheduling request to update
     * @param dynamicAppInfo dynamic application information
     */
    private void updateSchedulingRequest(ApplicationId appId,
        SchedulingRequest sr, DynamicAppInfo dynamicAppInfo) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Before updating scheduling request, appId={}, SR={}, conf={}",
            appId, sr, updateItemConf.toString());
      }
      if (executionType != null) {
        sr.setExecutionType(ExecutionTypeRequest.newInstance(executionType));
      }
      if (dynamicAppInfo.dynamicPlacementConstraint != null) {
        sr.setPlacementConstraint(dynamicAppInfo.dynamicPlacementConstraint);
      }
      if (dynamicAppInfo.dynamicAllocationTags != null) {
        sr.setAllocationTags(dynamicAppInfo.dynamicAllocationTags);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Done updating scheduling request, appId={}, SR={}", appId,
            sr);
      }
    }

    private PlacementConstraint getRuntimePlacementConstraint(
        Map<String, String> appInfo) {
      if (placementConstraint == null) {
        return null;
      }
      if (!hasPlaceholderForPC) {
        // return static placement constraint
        return placementConstraint;
      }
      try {
        // for dynamic placement constraint
        StringSubstitutor substitutor = new StringSubstitutor(appInfo);
        String substitutedPCExpression =
            substitutor.replace(updateItemConf.getPlacementConstraint());
        PlacementConstraint.AbstractConstraint absConstraint =
            PlacementConstraintParser.parseExpression(substitutedPCExpression);
        return new PlacementConstraint(absConstraint);
      } catch (PlacementConstraintParseException e) {
        LOG.warn("Failed to apply variable substitution to placement constraint. " +
            "Skip setting placement constraint.", e);
      }
      return null;
    }

    private Set<String> getRuntimeAllocationTags(Map<String, String> appInfo) {
      if (updateItemConf.getAllocationTags() == null ||
          updateItemConf.getAllocationTags().isEmpty()) {
        return null;
      }
      if (!hasPlaceholderForAllocTags) {
        // return static allocation tags
        return updateItemConf.getAllocationTags();
      }
      // for dynamic allocation tags
      StringSubstitutor substitutor = new StringSubstitutor(appInfo);
      return updateItemConf.getAllocationTags().stream()
          .map(substitutor::replace).collect(Collectors.toSet());
    }

    @VisibleForTesting
    public UpdateItemConf getUpdateItemConf() {
      return updateItemConf;
    }

    @VisibleForTesting
    public boolean hasPlaceholderForPC() {
      return hasPlaceholderForPC;
    }

    @VisibleForTesting
    public boolean hasPlaceholderForAllocTags() {
      return hasPlaceholderForAllocTags;
    }

    @VisibleForTesting
    public CompiledScript getAppMatchScript() {
      return appMatchScript;
    }

    @VisibleForTesting
    public CompiledScript getRequestMatchScript() {
      return requestMatchScript;
    }

    @VisibleForTesting
    public PlacementConstraint getPlacementConstraint() {
      return placementConstraint;
    }

    @VisibleForTesting
    public ExecutionType getExecutionType() {
      return executionType;
    }
  }

  private static Map<String, String> convertToStringAppInfo(
      Map<String, Object> appInfo) {
    return appInfo.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            e -> e.getValue() != null ? e.getValue().toString() : ""));
  }

  private static Map<String, Object> convertToAppInfo(FiCaSchedulerApp app,
      RMApp rmApp) {
    return ImmutableMap.of(APP_INFO_KEY_QUEUE, app.getQueueName(),
        APP_INFO_KEY_USER, app.getUser(),
        APP_INFO_KEY_PRIORITY, app.getPriority() == null ?
            0 : app.getPriority().getPriority(),
        APP_INFO_KEY_ID, rmApp.getApplicationId().toString(),
        APP_INFO_KEY_NAME, rmApp.getName(),
        APP_INFO_KEY_TYPE, rmApp.getApplicationType(),
        APP_INFO_KEY_TAGS, rmApp.getApplicationTags());
  }

  private static Map<String, Object> convertToRequestInfo(
      DynamicAppInfo dynamicAppInfo, ResourceRequest rr) {
    return ImmutableMap.of(REQUEST_INFO_KEY_PRIORITY,
        rr.getPriority() == null ? 0 : rr.getPriority().getPriority(),
        REQUEST_INFO_KEY_RESOURCE_NAME, rr.getResourceName(),
        REQUEST_INFO_KEY_RELAX_LOCALITY, rr.getRelaxLocality(),
        REQUEST_INFO_KEY_IS_AM, dynamicAppInfo.app.isWaitingForAMContainer());
  }

  private static Map<String, Object> convertToRequestInfo(
      DynamicAppInfo dynamicAppInfo, SchedulingRequest sr) {
    return ImmutableMap.of(REQUEST_INFO_KEY_PRIORITY,
        sr.getPriority() == null ? 0 : sr.getPriority().getPriority(),
        REQUEST_INFO_KEY_EXECUTION_TYPE, sr.getExecutionType() == null ?
            "" :
            (sr.getExecutionType().getExecutionType() == null ?
                "" :
                sr.getExecutionType().getExecutionType().name()),
        REQUEST_INFO_KEY_ALLOCATION_TAGS, sr.getAllocationTags(),
        REQUEST_INFO_KEY_IS_AM, dynamicAppInfo.app.isWaitingForAMContainer());
  }

  public static boolean hasPlaceholder(String text) throws YarnException {
    if (text == null) {
      return false;
    }
    // find out all placeholder
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(text);
    int placeholderCount = 0;
    while (matcher.find()) {
      String placeholder = matcher.group();
      String placeholderKey = placeholder.substring(2, placeholder.length() - 1);
      if (!VALID_PLACEHOLDER_KEYS.contains(placeholderKey)) {
        throw new YarnException("Invalid placeholder: " + placeholder);
      }
      placeholderCount++;
    }
    return placeholderCount > 0;
  }

  public static class DynamicAppInfo {
    private final FiCaSchedulerApp app;
    private final boolean isMatched;
    // dynamic placement constraint
    private final PlacementConstraint dynamicPlacementConstraint;
    // dynamic allocation tags
    private final Set<String> dynamicAllocationTags;

    public DynamicAppInfo(FiCaSchedulerApp app, boolean isMatched,
        PlacementConstraint dynamicPlacementConstraint,
        Set<String> dynamicAllocationTags) {
      this.app = app;
      this.isMatched = isMatched;
      this.dynamicPlacementConstraint = dynamicPlacementConstraint;
      this.dynamicAllocationTags = dynamicAllocationTags;
    }
  }
}
