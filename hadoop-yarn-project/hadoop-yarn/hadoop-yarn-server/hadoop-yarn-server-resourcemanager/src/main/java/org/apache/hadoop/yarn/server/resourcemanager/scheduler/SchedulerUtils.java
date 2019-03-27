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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
        .InvalidResourceType;
import org.apache.hadoop.yarn.exceptions
        .SchedulerInvalidResoureRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.UNKNOWN_REASON_MESSAGE_TEMPLATE;

/**
 * Utilities shared by schedulers. 
 */
@Private
@Unstable
public class SchedulerUtils {

  /**
   * This class contains invalid resource information along with its
   * resource request.
   */
  public static class MaxResourceValidationResult {
    private ResourceRequest resourceRequest;
    private List<ResourceInformation> invalidResources;

    MaxResourceValidationResult(ResourceRequest resourceRequest,
        List<ResourceInformation> invalidResources) {
      this.resourceRequest = resourceRequest;
      this.invalidResources = invalidResources;
    }

    public boolean isValid() {
      return invalidResources.isEmpty();
    }

    @Override
    public String toString() {
      return "MaxResourceValidationResult{" + "resourceRequest="
          + resourceRequest + ", invalidResources=" + invalidResources + '}';
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerUtils.class);

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  public static final String RELEASED_CONTAINER =
      "Container released by application";

  public static final String UPDATED_CONTAINER =
      "Temporary container killed by application for ExeutionType update";

  public static final String LOST_CONTAINER =
      "Container released on a *lost* node";

  public static final String PREEMPTED_CONTAINER =
      "Container preempted by scheduler";

  public static final String COMPLETED_APPLICATION =
      "Container of a completed application";

  public static final String EXPIRED_CONTAINER =
      "Container expired since it was unused";

  public static final String UNRESERVED_CONTAINER =
      "Container reservation no longer required.";

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  public static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.ABORTED, diagnostics);
  }


  /**
   * Utility to create a {@link ContainerStatus} for killed containers.
   * @param containerId {@link ContainerId} of the killed container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for a killed container
   */
  public static ContainerStatus createKilledContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.KILLED_BY_RESOURCEMANAGER, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost
   *         container
   */
  public static ContainerStatus createPreemptedContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.PREEMPTED, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  private static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, int exitStatus, String diagnostics) {
    ContainerStatus containerStatus =
        recordFactory.newRecordInstance(ContainerStatus.class);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setState(ContainerState.COMPLETE);
    return containerStatus;
  }

  /**
   * Utility method to normalize a resource request, by ensuring that the
   * requested memory is a multiple of minMemory and is not zero.
   */
  @VisibleForTesting
  public static void normalizeRequest(
    ResourceRequest ask,
    ResourceCalculator resourceCalculator,
    Resource minimumResource,
    Resource maximumResource) {
    ask.setCapability(
        getNormalizedResource(ask.getCapability(), resourceCalculator,
            minimumResource, maximumResource, minimumResource));
  }

  /**
   * Utility method to normalize a resource request, by ensuring that the
   * requested memory is a multiple of increment resource and is not zero.
   *
   * @return normalized resource
   */
  public static Resource getNormalizedResource(
      Resource ask,
      ResourceCalculator resourceCalculator,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    Resource normalized = Resources.normalize(
        resourceCalculator, ask, minimumResource,
        maximumResource, incrementResource);
    return normalized;
  }

  private static void normalizeNodeLabelExpressionInRequest(
      ResourceRequest resReq, QueueInfo queueInfo) {

    String labelExp = resReq.getNodeLabelExpression();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested Node Label Expression : " + labelExp);
      LOG.debug("Queue Info : " + queueInfo);
    }

    // if queue has default label expression, and RR doesn't have, use the
    // default label expression of queue
    if (labelExp == null && queueInfo != null && ResourceRequest.ANY
        .equals(resReq.getResourceName())) {
      LOG.debug("Setting default node label expression : {}", queueInfo
          .getDefaultNodeLabelExpression());
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    // If labelExp still equals to null, it could either be a dynamic queue
    // or the label is not configured
    // set it to be NO_LABEL in case of a pre-configured queue. Dynamic
    // queues are handled in RMAppAttemptImp.ScheduledTransition
    if (labelExp == null && queueInfo != null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }

    if (labelExp != null) {
      resReq.setNodeLabelExpression(labelExp);
    }
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    Configuration conf = rmContext.getYarnConfiguration();
    // If Node label is not enabled throw exception
    if (null != conf && !YarnConfiguration.areNodeLabelsEnabled(conf)) {
      String labelExp = resReq.getNodeLabelExpression();
      if (!(RMNodeLabelsManager.NO_LABEL.equals(labelExp)
          || null == labelExp)) {
        String message = "NodeLabel is not enabled in cluster, but resource"
            + " request contains a label expression.";
        LOG.warn(message);
        if (!isRecovery) {
          throw new InvalidLabelResourceRequestException(
              "Invalid resource request, node label not enabled "
                  + "but request contains label expression");
        }
      }
    }
    if (null == queueInfo) {
      try {
        queueInfo = scheduler.getQueueInfo(queueName, false, false);
      } catch (IOException e) {
        //Queue may not exist since it could be auto-created in case of
        // dynamic queues
      }
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);

    if (!isRecovery) {
      validateResourceRequest(resReq, maximumAllocation, queueInfo, rmContext);
    }
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, YarnScheduler scheduler,
      RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumAllocation, queueName, scheduler,
        false, rmContext, queueInfo);
  }

  /**
   * Utility method to validate a resource request, by ensuring that the
   * requested memory/vcore is non-negative and not greater than max
   *
   * @throws InvalidResourceRequestException when there is invalid request
   */
  private static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumAllocation, QueueInfo queueInfo, RMContext rmContext)
      throws InvalidResourceRequestException {
    final Resource requestedResource = resReq.getCapability();
    checkResourceRequestAgainstAvailableResource(requestedResource,
        maximumAllocation);

    String labelExp = resReq.getNodeLabelExpression();
    // we don't allow specify label expression other than resourceName=ANY now
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified node label expression in a "
              + "resource request has resource name = "
              + resReq.getResourceName());
    }

    // we don't allow specify label expression with more than one node labels now
    if (labelExp != null && labelExp.contains("&&")) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified more than one node label "
              + "in a node label expression, node label expression = "
              + labelExp);
    }

    if (labelExp != null && !labelExp.trim().isEmpty() && queueInfo != null) {
      if (!checkQueueLabelExpression(queueInfo.getAccessibleNodeLabels(),
          labelExp, rmContext)) {
        throw new InvalidLabelResourceRequestException(
            "Invalid resource request" + ", queue=" + queueInfo.getQueueName()
                + " doesn't have permission to access all labels "
                + "in resource request. labelExpression of resource request="
                + labelExp + ". Queue labels="
                + (queueInfo.getAccessibleNodeLabels() == null ? ""
                    : StringUtils.join(
                        queueInfo.getAccessibleNodeLabels().iterator(), ',')));
      } else {
        checkQueueLabelInLabelManager(labelExp, rmContext);
      }
    }
  }

  private static Map<String, ResourceInformation> getZeroResources(
      Resource resource) {
    Map<String, ResourceInformation> resourceInformations = Maps.newHashMap();
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation =
          resource.getResourceInformation(i);
      if (resourceInformation.getValue() == 0L) {
        resourceInformations.put(resourceInformation.getName(),
            resourceInformation);
      }
    }
    return resourceInformations;
  }

  @Private
  @VisibleForTesting
  static void checkResourceRequestAgainstAvailableResource(Resource reqResource,
      Resource availableResource) throws InvalidResourceRequestException {
    for (int i = 0; i < ResourceUtils.getNumberOfCountableResourceTypes(); i++) {
      final ResourceInformation requestedRI =
          reqResource.getResourceInformation(i);
      final String reqResourceName = requestedRI.getName();

      if (requestedRI.getValue() < 0) {
        throwInvalidResourceException(reqResource, availableResource,
            reqResourceName, InvalidResourceType.LESS_THAN_ZERO);
      }

      boolean valid = checkResource(requestedRI, availableResource);
      if (!valid) {
        throwInvalidResourceException(reqResource, availableResource,
            reqResourceName, InvalidResourceType.GREATER_THEN_MAX_ALLOCATION);
      }
    }
  }

  public static MaxResourceValidationResult
      validateResourceRequestsAgainstQueueMaxResource(
      ResourceRequest resReq, Resource availableResource)
      throws SchedulerInvalidResoureRequestException {
    final Resource reqResource = resReq.getCapability();
    Map<String, ResourceInformation> resourcesWithZeroAmount =
        getZeroResources(availableResource);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Resources with zero amount: "
          + Arrays.toString(resourcesWithZeroAmount.entrySet().toArray()));
    }

    List<ResourceInformation> invalidResources = Lists.newArrayList();
    for (int i = 0; i < ResourceUtils.getNumberOfCountableResourceTypes(); i++) {
      final ResourceInformation requestedRI =
          reqResource.getResourceInformation(i);
      final String reqResourceName = requestedRI.getName();

      if (resourcesWithZeroAmount.containsKey(reqResourceName)
          && requestedRI.getValue() > 0) {
        invalidResources.add(requestedRI);
      }
    }
    return new MaxResourceValidationResult(resReq, invalidResources);
  }

  /**
   * Checks requested ResouceInformation against available Resource.
   * @param requestedRI
   * @param availableResource
   * @return true if request is valid, false otherwise.
   */
  private static boolean checkResource(
      ResourceInformation requestedRI, Resource availableResource) {
    final ResourceInformation availableRI =
        availableResource.getResourceInformation(requestedRI.getName());

    long requestedResourceValue = requestedRI.getValue();
    long availableResourceValue = availableRI.getValue();
    int unitsRelation = UnitsConversionUtil.compareUnits(requestedRI.getUnits(),
        availableRI.getUnits());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested resource information: " + requestedRI);
      LOG.debug("Available resource information: " + availableRI);
      LOG.debug("Relation of units: " + unitsRelation);
    }

    // requested resource unit is less than available resource unit
    // e.g. requestedUnit: "m", availableUnit: "K")
    if (unitsRelation < 0) {
      availableResourceValue =
          UnitsConversionUtil.convert(availableRI.getUnits(),
              requestedRI.getUnits(), availableRI.getValue());

      // requested resource unit is greater than available resource unit
      // e.g. requestedUnit: "G", availableUnit: "M")
    } else if (unitsRelation > 0) {
      requestedResourceValue =
          UnitsConversionUtil.convert(requestedRI.getUnits(),
              availableRI.getUnits(), requestedRI.getValue());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested resource value after conversion: "
          + requestedResourceValue);
      LOG.info("Available resource value after conversion: "
          + availableResourceValue);
    }

    return requestedResourceValue <= availableResourceValue;
  }

  private static void throwInvalidResourceException(Resource reqResource,
          Resource maxAllowedAllocation, String reqResourceName,
          InvalidResourceType invalidResourceType)
      throws InvalidResourceRequestException {
    final String message;

    if (invalidResourceType == InvalidResourceType.LESS_THAN_ZERO) {
      message = String.format(LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE,
          reqResourceName, reqResource);
    } else if (invalidResourceType ==
            InvalidResourceType.GREATER_THEN_MAX_ALLOCATION) {
      message = String.format(GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE,
          reqResourceName, reqResource, maxAllowedAllocation,
          ResourceUtils.getResourceTypesMaximumAllocation());
    } else if (invalidResourceType == InvalidResourceType.UNKNOWN) {
      message = String.format(UNKNOWN_REASON_MESSAGE_TEMPLATE, reqResourceName,
          reqResource);
    } else {
      throw new IllegalArgumentException(String.format(
          "InvalidResourceType argument should be either " + "%s, %s or %s",
          InvalidResourceType.LESS_THAN_ZERO,
          InvalidResourceType.GREATER_THEN_MAX_ALLOCATION,
          InvalidResourceType.UNKNOWN));
    }
    throw new InvalidResourceRequestException(message, invalidResourceType);
  }

  private static void checkQueueLabelInLabelManager(String labelExpression,
      RMContext rmContext) throws InvalidLabelResourceRequestException {
    // check node label manager contains this label
    if (null != rmContext) {
      RMNodeLabelsManager nlm = rmContext.getNodeLabelManager();
      if (nlm != null && !nlm.containsNodeLabel(labelExpression)) {
        throw new InvalidLabelResourceRequestException(
            "Invalid label resource request, cluster do not contain "
                + ", label= " + labelExpression);
      }
    }
  }

  /**
   * Check queue label expression, check if node label in queue's
   * node-label-expression existed in clusterNodeLabels if rmContext != null
   */
  public static boolean checkQueueLabelExpression(Set<String> queueLabels,
      String labelExpression, RMContext rmContext) {
    // if label expression is empty, we can allocate container on any node
    if (labelExpression == null) {
      return true;
    }
    for (String str : labelExpression.split("&&")) {
      str = str.trim();
      if (!str.trim().isEmpty()) {
        // check queue label
        if (queueLabels == null) {
          return false;
        } else {
          if (!queueLabels.contains(str)
              && !queueLabels.contains(RMNodeLabelsManager.ANY)) {
            return false;
          }
        }
      }
    }
    return true;
  }


  public static AccessType toAccessType(QueueACL acl) {
    switch (acl) {
    case ADMINISTER_QUEUE:
      return AccessType.ADMINISTER_QUEUE;
    case SUBMIT_APPLICATIONS:
      return AccessType.SUBMIT_APP;
    }
    return null;
  }

  private static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String partitionToLookAt, Resource cluster) {
    if (Resources.greaterThan(rc, cluster,
        usage.getPending(partitionToLookAt), Resources.none())) {
      return true;
    }
    return false;
  }

  @Private
  public static boolean hasPendingResourceRequest(ResourceCalculator rc,
      ResourceUsage usage, String nodePartition, Resource cluster,
      SchedulingMode schedulingMode) {
    String partitionToLookAt = nodePartition;
    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      partitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }
    return hasPendingResourceRequest(rc, usage, partitionToLookAt, cluster);
  }

  public static RMContainer createOpportunisticRmContainer(RMContext rmContext,
      Container container, boolean isRemotelyAllocated) {
    SchedulerNode node = ((AbstractYarnScheduler) rmContext.getScheduler())
        .getNode(container.getNodeId());
    if (node == null) {
      return null;
    }
    SchedulerApplicationAttempt appAttempt =
        ((AbstractYarnScheduler) rmContext.getScheduler())
            .getCurrentAttemptForContainer(container.getId());
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container),
        appAttempt.getApplicationAttemptId(), container.getNodeId(),
        appAttempt.getUser(), rmContext, isRemotelyAllocated);
    appAttempt.addRMContainer(container.getId(), rmContainer);
    node.allocateContainer(rmContainer);
    return rmContainer;
  }
}
