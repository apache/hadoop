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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

/**
 * Utilities shared by schedulers. 
 */
@Private
@Unstable
public class SchedulerUtils {
  
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  public static final String RELEASED_CONTAINER = 
      "Container released by application";
  
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
   * Utility method to normalize a list of resource requests, by insuring that
   * the memory for each request is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequests(
    List<ResourceRequest> asks,
    ResourceCalculator resourceCalculator,
    Resource clusterResource,
    Resource minimumResource,
    Resource maximumResource) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(
        ask, resourceCalculator, clusterResource, minimumResource,
        maximumResource, minimumResource);
    }
  }

  /**
   * Utility method to normalize a resource request, by insuring that the
   * requested memory is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequest(
    ResourceRequest ask,
    ResourceCalculator resourceCalculator,
    Resource clusterResource,
    Resource minimumResource,
    Resource maximumResource) {
    Resource normalized =
      Resources.normalize(
        resourceCalculator, ask.getCapability(), minimumResource,
        maximumResource, minimumResource);
    ask.setCapability(normalized);
  }
  
  /**
   * Utility method to normalize a list of resource requests, by insuring that
   * the memory for each request is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequests(
      List<ResourceRequest> asks,
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(
          ask, resourceCalculator, clusterResource, minimumResource,
          maximumResource, incrementResource);
    }
  }

  /**
   * Utility method to normalize a resource request, by insuring that the
   * requested memory is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequest(
      ResourceRequest ask, 
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    Resource normalized = 
        Resources.normalize(
            resourceCalculator, ask.getCapability(), minimumResource,
            maximumResource, incrementResource);
    ask.setCapability(normalized);
  }

  private static void normalizeNodeLabelExpressionInRequest(
      ResourceRequest resReq, QueueInfo queueInfo) {

    String labelExp = resReq.getNodeLabelExpression();

    // if queue has default label expression, and RR doesn't have, use the
    // default label expression of queue
    if (labelExp == null && queueInfo != null && ResourceRequest.ANY
        .equals(resReq.getResourceName())) {
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    // If labelExp still equals to null, set it to be NO_LABEL
    if (labelExp == null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }
    resReq.setNodeLabelExpression(labelExp);
  }

  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {

    if (queueInfo == null) {
      try {
        queueInfo = scheduler.getQueueInfo(queueName, false, false);
      } catch (IOException e) {
        // it is possible queue cannot get when queue mapping is set, just ignore
        // the queueInfo here, and move forward
      }
    }
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);
    if (!isRecovery) {
      validateResourceRequest(resReq, maximumResource, queueInfo, rmContext);
    }
  }
  
  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      boolean isRecovery, RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        isRecovery, rmContext, null);
  }

  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext, QueueInfo queueInfo)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false, rmContext, queueInfo);
  }

  
  public static void normalizeAndvalidateRequest(ResourceRequest resReq,
      Resource maximumResource, String queueName, YarnScheduler scheduler,
      RMContext rmContext)
      throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler,
        false, rmContext, null);
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
    if (resReq.getCapability().getMemory() < 0 ||
        resReq.getCapability().getMemory() > maximumResource.getMemory()) {
      throw new InvalidResourceRequestException("Invalid resource request"
          + ", requested memory < 0"
          + ", or requested memory > max configured"
          + ", requestedMemory=" + resReq.getCapability().getMemory()
          + ", maxMemory=" + maximumResource.getMemory());
    }
    if (resReq.getCapability().getVirtualCores() < 0 ||
        resReq.getCapability().getVirtualCores() >
        maximumResource.getVirtualCores()) {
      throw new InvalidResourceRequestException("Invalid resource request"
          + ", requested virtual cores < 0"
          + ", or requested virtual cores > max configured"
          + ", requestedVirtualCores="
          + resReq.getCapability().getVirtualCores()
          + ", maxVirtualCores=" + maximumResource.getVirtualCores());
    }
    String labelExp = resReq.getNodeLabelExpression();

    // we don't allow specify label expression other than resourceName=ANY now
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {
      throw new InvalidResourceRequestException(
          "Invailid resource request, queue=" + queueInfo.getQueueName()
              + " specified node label expression in a "
              + "resource request has resource name = "
              + resReq.getResourceName());
    }
    
    // we don't allow specify label expression with more than one node labels now
    if (labelExp != null && labelExp.contains("&&")) {
      throw new InvalidResourceRequestException(
          "Invailid resource request, queue=" + queueInfo.getQueueName()
              + " specified more than one node label "
              + "in a node label expression, node label expression = "
              + labelExp);
    }
    
    if (labelExp != null && !labelExp.trim().isEmpty() && queueInfo != null) {
      if (!checkQueueLabelExpression(queueInfo.getAccessibleNodeLabels(),
          labelExp, rmContext)) {
        throw new InvalidResourceRequestException("Invalid resource request"
            + ", queue="
            + queueInfo.getQueueName()
            + " doesn't have permission to access all labels "
            + "in resource request. labelExpression of resource request="
            + labelExp
            + ". Queue labels="
            + (queueInfo.getAccessibleNodeLabels() == null ? "" : StringUtils.join(queueInfo
                .getAccessibleNodeLabels().iterator(), ',')));
      }
    }
  }
  
  public static boolean checkQueueAccessToNode(Set<String> queueLabels,
      Set<String> nodeLabels) {
    // if queue's label is *, it can access any node
    if (queueLabels != null && queueLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (queueLabels != null
        && Sets.intersection(queueLabels, nodeLabels).size() > 0) {
      return true;
    }
    // sorry, you cannot access
    return false;
  }
  
  public static void checkIfLabelInClusterNodeLabels(RMNodeLabelsManager mgr,
      Set<String> labels) throws IOException {
    if (mgr == null) {
      if (labels != null && !labels.isEmpty()) {
        throw new IOException("NodeLabelManager is null, please check");
      }
      return;
    }

    if (labels != null) {
      for (String label : labels) {
        if (!label.equals(RMNodeLabelsManager.ANY)
            && !mgr.containsNodeLabel(label)) {
          throw new IOException("NodeLabelManager doesn't include label = "
              + label + ", please check.");
        }
      }
    }
  }
  
  public static boolean checkNodeLabelExpression(Set<String> nodeLabels,
      String labelExpression) {
    // empty label expression can only allocate on node with empty labels
    if (labelExpression == null || labelExpression.trim().isEmpty()) {
      if (!nodeLabels.isEmpty()) {
        return false;
      }
    }

    if (labelExpression != null) {
      for (String str : labelExpression.split("&&")) {
        if (!str.trim().isEmpty()
            && (nodeLabels == null || !nodeLabels.contains(str.trim()))) {
          return false;
        }
      }
    }
    return true;
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
        
        // check node label manager contains this label
        if (null != rmContext) {
          RMNodeLabelsManager nlm = rmContext.getNodeLabelManager();
          if (nlm != null && !nlm.containsNodeLabel(str)) {
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
}
