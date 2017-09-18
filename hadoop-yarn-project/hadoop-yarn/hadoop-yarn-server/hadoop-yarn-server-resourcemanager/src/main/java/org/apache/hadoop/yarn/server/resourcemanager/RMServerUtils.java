/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ProfileCapability;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions
    .InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Utility methods to aid serving RM data through the REST and RPC APIs
 */
public class RMServerUtils {

  private static final Log LOG_HANDLE = LogFactory.getLog(RMServerUtils.class);

  public static final String UPDATE_OUTSTANDING_ERROR =
      "UPDATE_OUTSTANDING_ERROR";
  private static final String INCORRECT_CONTAINER_VERSION_ERROR =
      "INCORRECT_CONTAINER_VERSION_ERROR";
  private static final String INVALID_CONTAINER_ID =
      "INVALID_CONTAINER_ID";
  private static final String RESOURCE_OUTSIDE_ALLOWED_RANGE =
      "RESOURCE_OUTSIDE_ALLOWED_RANGE";

  protected static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  private static Clock clock = SystemClock.getInstance();

  public static List<RMNode> queryRMNodes(RMContext context,
      EnumSet<NodeState> acceptedStates) {
    // nodes contains nodes that are NEW, RUNNING, UNHEALTHY or DECOMMISSIONING.
    ArrayList<RMNode> results = new ArrayList<RMNode>();
    if (acceptedStates.contains(NodeState.NEW) ||
        acceptedStates.contains(NodeState.RUNNING) ||
        acceptedStates.contains(NodeState.DECOMMISSIONING) ||
        acceptedStates.contains(NodeState.UNHEALTHY)) {
      for (RMNode rmNode : context.getRMNodes().values()) {
        if (acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }

    // inactiveNodes contains nodes that are DECOMMISSIONED, LOST, OR REBOOTED
    if (acceptedStates.contains(NodeState.DECOMMISSIONED) ||
        acceptedStates.contains(NodeState.LOST) ||
        acceptedStates.contains(NodeState.REBOOTED)) {
      for (RMNode rmNode : context.getInactiveRMNodes().values()) {
        if ((rmNode != null) && acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }
    return results;
  }

  /**
   * Check if we have:
   * - Request for same containerId and different target resource.
   * - If targetResources violates maximum/minimumAllocation.
   * @param rmContext RM context.
   * @param request Allocate Request.
   * @param maximumAllocation Maximum Allocation.
   * @param updateErrors Container update errors.
   * @return ContainerUpdateRequests.
   */
  public static ContainerUpdates
      validateAndSplitUpdateResourceRequests(RMContext rmContext,
      AllocateRequest request, Resource maximumAllocation,
      List<UpdateContainerError> updateErrors) {
    ContainerUpdates updateRequests =
        new ContainerUpdates();
    Set<ContainerId> outstandingUpdate = new HashSet<>();
    for (UpdateContainerRequest updateReq : request.getUpdateRequests()) {
      RMContainer rmContainer = rmContext.getScheduler().getRMContainer(
          updateReq.getContainerId());
      String msg = validateContainerIdAndVersion(outstandingUpdate,
          updateReq, rmContainer);
      ContainerUpdateType updateType = updateReq.getContainerUpdateType();
      if (msg == null) {
        if ((updateType != ContainerUpdateType.PROMOTE_EXECUTION_TYPE) &&
            (updateType !=ContainerUpdateType.DEMOTE_EXECUTION_TYPE)) {
          if (validateIncreaseDecreaseRequest(
              rmContext, updateReq, maximumAllocation)) {
            if (ContainerUpdateType.INCREASE_RESOURCE == updateType) {
              updateRequests.getIncreaseRequests().add(updateReq);
            } else {
              updateRequests.getDecreaseRequests().add(updateReq);
            }
            outstandingUpdate.add(updateReq.getContainerId());
          } else {
            msg = RESOURCE_OUTSIDE_ALLOWED_RANGE;
          }
        } else {
          ExecutionType original = rmContainer.getExecutionType();
          ExecutionType target = updateReq.getExecutionType();
          if (target != original) {
            if (target == ExecutionType.GUARANTEED &&
                original == ExecutionType.OPPORTUNISTIC) {
              updateRequests.getPromotionRequests().add(updateReq);
              outstandingUpdate.add(updateReq.getContainerId());
            } else if (target == ExecutionType.OPPORTUNISTIC &&
                original == ExecutionType.GUARANTEED) {
              updateRequests.getDemotionRequests().add(updateReq);
              outstandingUpdate.add(updateReq.getContainerId());
            }
          }
        }
      }
      checkAndcreateUpdateError(updateErrors, updateReq, rmContainer, msg);
    }
    return updateRequests;
  }

  private static void checkAndcreateUpdateError(
      List<UpdateContainerError> errors, UpdateContainerRequest updateReq,
      RMContainer rmContainer, String msg) {
    if (msg != null) {
      UpdateContainerError updateError = RECORD_FACTORY
          .newRecordInstance(UpdateContainerError.class);
      updateError.setReason(msg);
      updateError.setUpdateContainerRequest(updateReq);
      if (rmContainer != null) {
        updateError.setCurrentContainerVersion(
            rmContainer.getContainer().getVersion());
      } else {
        updateError.setCurrentContainerVersion(-1);
      }
      errors.add(updateError);
    }
  }

  private static String validateContainerIdAndVersion(
      Set<ContainerId> outstandingUpdate, UpdateContainerRequest updateReq,
      RMContainer rmContainer) {
    String msg = null;
    if (rmContainer == null) {
      msg = INVALID_CONTAINER_ID;
    }
    // Only allow updates if the requested version matches the current
    // version
    if (msg == null && updateReq.getContainerVersion() !=
        rmContainer.getContainer().getVersion()) {
      msg = INCORRECT_CONTAINER_VERSION_ERROR;
    }
    // No more than 1 container update per request.
    if (msg == null &&
        outstandingUpdate.contains(updateReq.getContainerId())) {
      msg = UPDATE_OUTSTANDING_ERROR;
    }
    return msg;
  }

  /**
   * Utility method to validate a list resource requests, by insuring that the
   * requested memory/vcore is non-negative and not greater than max
   */
  public static void normalizeAndValidateRequests(List<ResourceRequest> ask,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    // Get queue from scheduler
    QueueInfo queueInfo = null;
    try {
      queueInfo = scheduler.getQueueInfo(queueName, false, false);
    } catch (IOException e) {
    }

    for (ResourceRequest resReq : ask) {
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maximumResource,
          queueName, scheduler, rmContext, queueInfo);
    }
  }

  /**
   * Validate increase/decrease request.
   * <pre>
   * - Throw exception when any other error happens
   * </pre>
   */
  public static void checkSchedContainerChangeRequest(
      SchedContainerChangeRequest request, boolean increase)
      throws InvalidResourceRequestException {
    RMContext rmContext = request.getRmContext();
    ContainerId containerId = request.getContainerId();
    RMContainer rmContainer = request.getRMContainer();
    Resource targetResource = request.getTargetCapacity();

    // Compare targetResource and original resource
    Resource originalResource = rmContainer.getAllocatedResource();

    // Resource comparasion should be >= (or <=) for all resource vectors, for
    // example, you cannot request target resource of a <10G, 10> container to
    // <20G, 8>
    if (increase) {
      if (originalResource.getMemorySize() > targetResource.getMemorySize()
          || originalResource.getVirtualCores() > targetResource
          .getVirtualCores()) {
        String msg =
            "Trying to increase a container, but target resource has some"
                + " resource < original resource, target=" + targetResource
                + " original=" + originalResource + " containerId="
                + containerId;
        throw new InvalidResourceRequestException(msg);
      }
    } else {
      if (originalResource.getMemorySize() < targetResource.getMemorySize()
          || originalResource.getVirtualCores() < targetResource
          .getVirtualCores()) {
        String msg =
            "Trying to decrease a container, but target resource has "
                + "some resource > original resource, target=" + targetResource
                + " original=" + originalResource + " containerId="
                + containerId;
        throw new InvalidResourceRequestException(msg);
      }
    }

    // Target resource of the increase request is more than NM can offer
    ResourceScheduler scheduler = rmContext.getScheduler();
    RMNode rmNode = request.getSchedulerNode().getRMNode();
    if (!Resources.fitsIn(scheduler.getResourceCalculator(), targetResource,
        rmNode.getTotalCapability())) {
      String msg = "Target resource=" + targetResource + " of containerId="
          + containerId + " is more than node's total resource="
          + rmNode.getTotalCapability();
      throw new InvalidResourceRequestException(msg);
    }
  }

  /*
   * @throw <code>InvalidResourceBlacklistRequestException </code> if the
   * resource is not able to be added to the blacklist.
   */
  public static void validateBlacklistRequest(
      ResourceBlacklistRequest blacklistRequest)
      throws InvalidResourceBlacklistRequestException {
    if (blacklistRequest != null) {
      List<String> plus = blacklistRequest.getBlacklistAdditions();
      if (plus != null && plus.contains(ResourceRequest.ANY)) {
        throw new InvalidResourceBlacklistRequestException(
            "Cannot add " + ResourceRequest.ANY + " to the blacklist!");
      }
    }
  }

  // Sanity check and normalize target resource
  private static boolean validateIncreaseDecreaseRequest(RMContext rmContext,
      UpdateContainerRequest request, Resource maximumAllocation) {
    if (request.getCapability().getMemorySize() < 0
        || request.getCapability().getMemorySize() > maximumAllocation
        .getMemorySize()) {
      return false;
    }
    if (request.getCapability().getVirtualCores() < 0
        || request.getCapability().getVirtualCores() > maximumAllocation
        .getVirtualCores()) {
      return false;
    }
    ResourceScheduler scheduler = rmContext.getScheduler();
    request.setCapability(scheduler.getNormalizedResource(request.getCapability()));
    return true;
  }

  /**
   * It will validate to make sure all the containers belong to correct
   * application attempt id. If not then it will throw
   * {@link InvalidContainerReleaseException}
   *
   * @param containerReleaseList containers to be released as requested by
   *                             application master.
   * @param appAttemptId         Application attempt Id
   * @throws InvalidContainerReleaseException
   */
  public static void
      validateContainerReleaseRequest(List<ContainerId> containerReleaseList,
      ApplicationAttemptId appAttemptId)
      throws InvalidContainerReleaseException {
    for (ContainerId cId : containerReleaseList) {
      if (!appAttemptId.equals(cId.getApplicationAttemptId())) {
        throw new InvalidContainerReleaseException(
            "Cannot release container : "
                + cId.toString()
                + " not belonging to this application attempt : "
                + appAttemptId);
      }
    }
  }

  public static UserGroupInformation verifyAdminAccess(
      YarnAuthorizationProvider authorizer, String method, final Log LOG)
      throws IOException {
    // by default, this method will use AdminService as module name
    return verifyAdminAccess(authorizer, method, "AdminService", LOG);
  }

  /**
   * Utility method to verify if the current user has access based on the
   * passed {@link AccessControlList}
   *
   * @param authorizer the {@link AccessControlList} to check against
   * @param method     the method name to be logged
   * @param module     like AdminService or NodeLabelManager
   * @param LOG        the logger to use
   * @return {@link UserGroupInformation} of the current user
   * @throws IOException
   */
  public static UserGroupInformation verifyAdminAccess(
      YarnAuthorizationProvider authorizer, String method, String module,
      final Log LOG)
      throws IOException {
    UserGroupInformation user;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);
      RMAuditLogger.logFailure("UNKNOWN", method, "",
          "AdminService", "Couldn't get current user");
      throw ioe;
    }

    if (!authorizer.isAdmin(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission" +
          " to call '" + method + "'");

      RMAuditLogger.logFailure(user.getShortUserName(), method, "", module,
          RMAuditLogger.AuditConstants.UNAUTHORIZED_USER);

      throw new AccessControlException("User " + user.getShortUserName() +
          " doesn't have permission" +
          " to call '" + method + "'");
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(method + " invoked by user " + user.getShortUserName());
    }
    return user;
  }

  public static YarnApplicationState createApplicationState(
      RMAppState rmAppState) {
    switch (rmAppState) {
    case NEW:
      return YarnApplicationState.NEW;
    case NEW_SAVING:
      return YarnApplicationState.NEW_SAVING;
    case SUBMITTED:
      return YarnApplicationState.SUBMITTED;
    case ACCEPTED:
      return YarnApplicationState.ACCEPTED;
    case RUNNING:
      return YarnApplicationState.RUNNING;
    case FINISHING:
    case FINISHED:
      return YarnApplicationState.FINISHED;
    case KILLING:
    case KILLED:
      return YarnApplicationState.KILLED;
    case FAILED:
      return YarnApplicationState.FAILED;
    default:
      throw new YarnRuntimeException("Unknown state passed!");
    }
  }

  public static YarnApplicationAttemptState createApplicationAttemptState(
      RMAppAttemptState rmAppAttemptState) {
    switch (rmAppAttemptState) {
    case NEW:
      return YarnApplicationAttemptState.NEW;
    case SUBMITTED:
      return YarnApplicationAttemptState.SUBMITTED;
    case SCHEDULED:
      return YarnApplicationAttemptState.SCHEDULED;
    case ALLOCATED:
      return YarnApplicationAttemptState.ALLOCATED;
    case LAUNCHED:
      return YarnApplicationAttemptState.LAUNCHED;
    case ALLOCATED_SAVING:
    case LAUNCHED_UNMANAGED_SAVING:
      return YarnApplicationAttemptState.ALLOCATED_SAVING;
    case RUNNING:
      return YarnApplicationAttemptState.RUNNING;
    case FINISHING:
      return YarnApplicationAttemptState.FINISHING;
    case FINISHED:
      return YarnApplicationAttemptState.FINISHED;
    case KILLED:
      return YarnApplicationAttemptState.KILLED;
    case FAILED:
      return YarnApplicationAttemptState.FAILED;
    default:
      throw new YarnRuntimeException("Unknown state passed!");
    }
  }

  /**
   * Statically defined dummy ApplicationResourceUsageREport.  Used as
   * a return value when a valid report cannot be found.
   */
  public static final ApplicationResourceUsageReport
      DUMMY_APPLICATION_RESOURCE_USAGE_REPORT =
      BuilderUtils.newApplicationResourceUsageReport(-1, -1,
          Resources.createResource(-1, -1), Resources.createResource(-1, -1),
          Resources.createResource(-1, -1), new HashMap<>(), new HashMap<>());


  /**
   * Find all configs whose name starts with
   * YarnConfiguration.RM_PROXY_USER_PREFIX, and add a record for each one by
   * replacing the prefix with ProxyUsers.CONF_HADOOP_PROXYUSER
   */
  public static void processRMProxyUsersConf(Configuration conf) {
    Map<String, String> rmProxyUsers = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : conf) {
      String propName = entry.getKey();
      if (propName.startsWith(YarnConfiguration.RM_PROXY_USER_PREFIX)) {
        rmProxyUsers.put(ProxyUsers.CONF_HADOOP_PROXYUSER + "." +
                propName.substring(YarnConfiguration.RM_PROXY_USER_PREFIX
                    .length()),
            entry.getValue());
      }
    }
    for (Map.Entry<String, String> entry : rmProxyUsers.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public static void validateApplicationTimeouts(
      Map<ApplicationTimeoutType, Long> timeouts) throws YarnException {
    if (timeouts != null) {
      for (Map.Entry<ApplicationTimeoutType, Long> timeout : timeouts
          .entrySet()) {
        if (timeout.getValue() <= 0) {
          String message = "Invalid application timeout, value="
              + timeout.getValue() + " for type=" + timeout.getKey();
          throw new YarnException(message);
        }
      }
    }
  }

  /**
   * Validate ISO8601 format with epoch time.
   * @param timeoutsInISO8601 format
   * @return expire time in local epoch
   * @throws YarnException if given application timeout value is lesser than
   *           current time.
   */
  public static Map<ApplicationTimeoutType, Long> validateISO8601AndConvertToLocalTimeEpoch(
      Map<ApplicationTimeoutType, String> timeoutsInISO8601)
      throws YarnException {
    long currentTimeMillis = clock.getTime();
    Map<ApplicationTimeoutType, Long> newApplicationTimeout =
        new HashMap<ApplicationTimeoutType, Long>();
    if (timeoutsInISO8601 != null) {
      for (Map.Entry<ApplicationTimeoutType, String> timeout : timeoutsInISO8601
          .entrySet()) {
        long expireTime = 0L;
        try {
          expireTime =
              Times.parseISO8601ToLocalTimeInMillis(timeout.getValue());
        } catch (ParseException ex) {
          String message =
              "Expire time is not in ISO8601 format. ISO8601 supported "
                  + "format is yyyy-MM-dd'T'HH:mm:ss.SSSZ. Configured "
                  + "timeout value is " + timeout.getValue();
          throw new YarnException(message, ex);
        }
        if (expireTime < currentTimeMillis) {
          String message =
              "Expire time is less than current time, current-time="
                  + Times.formatISO8601(currentTimeMillis) + " expire-time="
                  + Times.formatISO8601(expireTime);
          throw new YarnException(message);
        }
        newApplicationTimeout.put(timeout.getKey(), expireTime);
      }
    }
    return newApplicationTimeout;
  }

  /**
   * Get applicable Node count for AM.
   *
   * @param rmContext context
   * @param conf configuration
   * @param amReqs am resource requests
   * @return applicable node count
   */
  public static int getApplicableNodeCountForAM(RMContext rmContext,
      Configuration conf, List<ResourceRequest> amReqs) {
    // Determine the list of nodes that are eligible based on the strict
    // resource requests
    Set<NodeId> nodesForReqs = new HashSet<>();
    for (ResourceRequest amReq : amReqs) {
      if (amReq.getRelaxLocality() &&
          !amReq.getResourceName().equals(ResourceRequest.ANY)) {
        nodesForReqs.addAll(
            rmContext.getScheduler().getNodeIds(amReq.getResourceName()));
      }
    }

    if (YarnConfiguration.areNodeLabelsEnabled(conf)) {
      // Determine the list of nodes that are eligible based on the node label
      String amNodeLabelExpression = amReqs.get(0).getNodeLabelExpression();
      Set<NodeId> nodesForLabels =
          getNodeIdsForLabel(rmContext, amNodeLabelExpression);
      if (nodesForLabels != null && !nodesForLabels.isEmpty()) {
        // If only node labels, strip out any wildcard NodeIds and return
        if (nodesForReqs.isEmpty()) {
          for (Iterator<NodeId> it = nodesForLabels.iterator(); it.hasNext();) {
            if (it.next().getPort() == 0) {
              it.remove();
            }
          }
          return nodesForLabels.size();
        } else {
          // The NodeIds common to both the strict resource requests and the
          // node label is the eligible set
          return Sets.intersection(nodesForReqs, nodesForLabels).size();
        }
      }
    }

    // If no strict resource request NodeIds nor node label NodeIds, then just
    // return the entire cluster
    if (nodesForReqs.isEmpty()) {
      return rmContext.getScheduler().getNumClusterNodes();
    }
    // No node label NodeIds, so return the strict resource request NodeIds
    return nodesForReqs.size();
  }

  private static Set<NodeId> getNodeIdsForLabel(RMContext rmContext,
      String label) {
    label = (label == null || label.trim().isEmpty())
        ? RMNodeLabelsManager.NO_LABEL : label;
    if (label.equals(RMNodeLabelsManager.NO_LABEL)) {
      // NO_LABEL nodes aren't tracked directly
      return rmContext.getNodeLabelManager().getNodesWithoutALabel();
    } else {
      Map<String, Set<NodeId>> labelsToNodes =
          rmContext.getNodeLabelManager().getLabelsToNodes(
              Collections.singleton(label));
      return labelsToNodes.get(label);
    }
  }

  public static void convertProfileToResourceCapability(ResourceRequest ask,
      Configuration conf, ResourceProfilesManager resourceProfilesManager)
      throws YarnException {

    if (LOG_HANDLE.isDebugEnabled()) {
      LOG_HANDLE
          .debug("Converting profile to resource capability for ask " + ask);
    }

    boolean profilesEnabled =
        conf.getBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
            YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED);
    if (!profilesEnabled) {
      if (ask.getProfileCapability() != null && !ask.getProfileCapability()
          .getProfileCapabilityOverride().equals(Resources.none())) {
        ask.setCapability(
            ask.getProfileCapability().getProfileCapabilityOverride());
      }
    } else {
      if (ask.getProfileCapability() != null) {
        ask.setCapability(ProfileCapability
            .toResource(ask.getProfileCapability(),
                resourceProfilesManager.getResourceProfiles()));
      }
    }
    if (LOG_HANDLE.isDebugEnabled()) {
      LOG_HANDLE
          .debug("Converted profile to resource capability for ask " + ask);
    }
  }

  public static Long getOrDefault(Map<String, Long> map, String key,
      Long defaultValue) {
    if (map.containsKey(key)) {
      return map.get(key);
    }
    return defaultValue;
  }
}
