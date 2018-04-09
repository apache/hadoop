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
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Utilities shared by schedulers. 
 */
@Private
@Unstable
public class SchedulerUtils {

  private static final Log LOG = LogFactory.getLog(SchedulerUtils.class);

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
   * Utility method to normalize a resource request, by insuring that the
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
   * Utility method to normalize a resource request, by insuring that the
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
      if ( LOG.isDebugEnabled()) {
        LOG.debug("Setting default node label expression : " + queueInfo
            .getDefaultNodeLabelExpression());
      }
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    // If labelExp still equals to null, it could either be a dynamic queue
    // or the label is not configured
    // set it to be NO_LABEL in case of a pre-configured queue. Dynamic
    // queues are handled in RMAppAttemptImp.ScheduledTransition
    if (labelExp == null && queueInfo != null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }

    if ( labelExp != null) {
      resReq.setNodeLabelExpression(labelExp);
    }
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        isRecovery, rmContext, null);
  }


  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
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
      validateResourceRequest(resReq, maximumResource, queueInfo, rmContext);
    }
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext) throws InvalidResourceRequestException {
    normalizeAndvalidateRequest(resReq, maximumResource, queueName, scheduler,
        rmContext, null);
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false, rmContext, queueInfo);
  }

  /**
   * Utility method to validate a resource request, by insuring that the
   * requested memory/vcore is non-negative and not greater than max
   * 
   * @throws InvalidResourceRequestException when there is invalid request
   */
  private static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumResource, QueueInfo queueInfo, RMContext rmContext)
      throws InvalidResourceRequestException {
    Resource requestedResource = resReq.getCapability();
    for (int i = 0; i < ResourceUtils.getNumberOfKnownResourceTypes(); i++) {
      ResourceInformation reqRI = requestedResource.getResourceInformation(i);
      ResourceInformation maxRI = maximumResource.getResourceInformation(i);
      if (reqRI.getValue() < 0 || reqRI.getValue() > maxRI.getValue()) {
        throw new InvalidResourceRequestException(
            "Invalid resource request, requested resource type=[" + reqRI
                .getName()
                + "] < 0 or greater than maximum allowed allocation. Requested "
                + "resource=" + requestedResource
                + ", maximum allowed allocation=" + maximumResource
                + ", please note that maximum allowed allocation is calculated "
                + "by scheduler based on maximum resource of registered "
                + "NodeManagers, which might be less than configured "
                + "maximum allocation=" + ResourceUtils
                .getResourceTypesMaximumAllocation());
      }
    }
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
    SchedulerApplicationAttempt appAttempt =
        ((AbstractYarnScheduler) rmContext.getScheduler())
            .getCurrentAttemptForContainer(container.getId());
    RMContainer rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container),
        appAttempt.getApplicationAttemptId(), container.getNodeId(),
        appAttempt.getUser(), rmContext, isRemotelyAllocated);
    appAttempt.addRMContainer(container.getId(), rmContainer);
    ((AbstractYarnScheduler) rmContext.getScheduler()).getNode(
        container.getNodeId()).allocateContainer(rmContainer);
    return rmContainer;
  }
}
