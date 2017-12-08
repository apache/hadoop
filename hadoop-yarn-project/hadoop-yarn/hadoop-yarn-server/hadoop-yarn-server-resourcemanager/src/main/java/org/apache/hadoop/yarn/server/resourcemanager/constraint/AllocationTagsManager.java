/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.resourcemanager.constraint;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongBinaryOperator;

/**
 * Support storing maps between container-tags/applications and
 * nodes. This will be required by affinity/anti-affinity implementation and
 * cardinality.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AllocationTagsManager {

  private static final Logger LOG = Logger.getLogger(
      AllocationTagsManager.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;

  // Application's tags to node
  private Map<ApplicationId, NodeToCountedTags> perAppMappings =
      new HashMap<>();

  // Global tags to node mapping (used to fast return aggregated tags
  // cardinality across apps)
  private NodeToCountedTags globalMapping = new NodeToCountedTags();

  /**
   * Store node to counted tags.
   */
  @VisibleForTesting
  static class NodeToCountedTags {
    // Map<NodeId, Map<Tag, Count>>
    private Map<NodeId, Map<String, Long>> nodeToTagsWithCount =
        new HashMap<>();

    // protected by external locks
    private void addTagsToNode(NodeId nodeId, Set<String> tags) {
      Map<String, Long> innerMap = nodeToTagsWithCount.computeIfAbsent(nodeId,
          k -> new HashMap<>());

      for (String tag : tags) {
        Long count = innerMap.get(tag);
        if (count == null) {
          innerMap.put(tag, 1L);
        } else{
          innerMap.put(tag, count + 1);
        }
      }
    }

    // protected by external locks
    private void addTagToNode(NodeId nodeId, String tag) {
      Map<String, Long> innerMap = nodeToTagsWithCount.computeIfAbsent(nodeId,
          k -> new HashMap<>());

      Long count = innerMap.get(tag);
      if (count == null) {
        innerMap.put(tag, 1L);
      } else{
        innerMap.put(tag, count + 1);
      }
    }

    private void removeTagFromInnerMap(Map<String, Long> innerMap, String tag) {
      Long count = innerMap.get(tag);
      if (count > 1) {
        innerMap.put(tag, count - 1);
      } else {
        if (count <= 0) {
          LOG.warn(
              "Trying to remove tags from node, however the count already"
                  + " becomes 0 or less, it could be a potential bug.");
        }
        innerMap.remove(tag);
      }
    }

    private void removeTagsFromNode(NodeId nodeId, Set<String> tags) {
      Map<String, Long> innerMap = nodeToTagsWithCount.get(nodeId);
      if (innerMap == null) {
        LOG.warn("Failed to find node=" + nodeId
            + " while trying to remove tags, please double check.");
        return;
      }

      for (String tag : tags) {
        removeTagFromInnerMap(innerMap, tag);
      }

      if (innerMap.isEmpty()) {
        nodeToTagsWithCount.remove(nodeId);
      }
    }

    private void removeTagFromNode(NodeId nodeId, String tag) {
      Map<String, Long> innerMap = nodeToTagsWithCount.get(nodeId);
      if (innerMap == null) {
        LOG.warn("Failed to find node=" + nodeId
            + " while trying to remove tags, please double check.");
        return;
      }

      removeTagFromInnerMap(innerMap, tag);

      if (innerMap.isEmpty()) {
        nodeToTagsWithCount.remove(nodeId);
      }
    }

    private long getCardinality(NodeId nodeId, String tag) {
      Map<String, Long> innerMap = nodeToTagsWithCount.get(nodeId);
      if (innerMap == null) {
        return 0;
      }
      Long value = innerMap.get(tag);
      return value == null ? 0 : value;
    }

    private long getCardinality(NodeId nodeId, Set<String> tags,
        LongBinaryOperator op) {
      Map<String, Long> innerMap = nodeToTagsWithCount.get(nodeId);
      if (innerMap == null) {
        return 0;
      }

      long returnValue = 0;
      boolean firstTag = true;

      if (tags != null && !tags.isEmpty()) {
        for (String tag : tags) {
          Long value = innerMap.get(tag);
          if (value == null) {
            value = 0L;
          }

          if (firstTag) {
            returnValue = value;
            firstTag = false;
            continue;
          }

          returnValue = op.applyAsLong(returnValue, value);
        }
      } else {
        // Similar to above if, but only iterate values for better performance
        for (long value : innerMap.values()) {
          // For the first value, we will not apply op
          if (firstTag) {
            returnValue = value;
            firstTag = false;
            continue;
          }
          returnValue = op.applyAsLong(returnValue, value);
        }
      }
      return returnValue;
    }

    private boolean isEmpty() {
      return nodeToTagsWithCount.isEmpty();
    }

    @VisibleForTesting
    public Map<NodeId, Map<String, Long>> getNodeToTagsWithCount() {
      return nodeToTagsWithCount;
    }
  }

  @VisibleForTesting
  Map<ApplicationId, NodeToCountedTags> getPerAppMappings() {
    return perAppMappings;
  }

  @VisibleForTesting
  NodeToCountedTags getGlobalMapping() {
    return globalMapping;
  }

  public AllocationTagsManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * Notify container allocated on a node.
   *
   * @param nodeId         allocated node.
   * @param applicationId  applicationId
   * @param containerId    container id.
   * @param allocationTags allocation tags, see
   *                       {@link SchedulingRequest#getAllocationTags()}
   *                       application_id will be added to allocationTags.
   */
  public void addContainer(NodeId nodeId, ApplicationId applicationId,
      ContainerId containerId, Set<String> allocationTags) {
    String applicationIdTag =
        AllocationTagsNamespaces.APP_ID + applicationId.toString();

    boolean useSet = false;
    if (allocationTags != null && !allocationTags.isEmpty()) {
      // Copy before edit it.
      allocationTags = new HashSet<>(allocationTags);
      allocationTags.add(applicationIdTag);
      useSet = true;
    }

    writeLock.lock();
    try {
      NodeToCountedTags perAppTagsMapping = perAppMappings.computeIfAbsent(
          applicationId, k -> new NodeToCountedTags());

      if (useSet) {
        perAppTagsMapping.addTagsToNode(nodeId, allocationTags);
        globalMapping.addTagsToNode(nodeId, allocationTags);
      } else {
        perAppTagsMapping.addTagToNode(nodeId, applicationIdTag);
        globalMapping.addTagToNode(nodeId, applicationIdTag);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Added container=" + containerId + " with tags=[" + StringUtils
                .join(allocationTags, ",") + "]");
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Notify container removed.
   *
   * @param nodeId         nodeId
   * @param applicationId  applicationId
   * @param containerId    containerId.
   * @param allocationTags allocation tags for given container
   */
  public void removeContainer(NodeId nodeId, ApplicationId applicationId,
      ContainerId containerId, Set<String> allocationTags) {
    String applicationIdTag =
        AllocationTagsNamespaces.APP_ID + applicationId.toString();
    boolean useSet = false;

    if (allocationTags != null && !allocationTags.isEmpty()) {
      // Copy before edit it.
      allocationTags = new HashSet<>(allocationTags);
      allocationTags.add(applicationIdTag);
      useSet = true;
    }

    writeLock.lock();
    try {
      NodeToCountedTags perAppTagsMapping = perAppMappings.get(applicationId);
      if (perAppTagsMapping == null) {
        return;
      }

      if (useSet) {
        perAppTagsMapping.removeTagsFromNode(nodeId, allocationTags);
        globalMapping.removeTagsFromNode(nodeId, allocationTags);
      } else {
        perAppTagsMapping.removeTagFromNode(nodeId, applicationIdTag);
        globalMapping.removeTagFromNode(nodeId, applicationIdTag);
      }

      if (perAppTagsMapping.isEmpty()) {
        perAppMappings.remove(applicationId);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Removed container=" + containerId + " with tags=[" + StringUtils
                .join(allocationTags, ",") + "]");
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get cardinality for following conditions. External can pass-in a binary op
   * to implement customized logic.   *
   * @param nodeId        nodeId, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all nodes.
   * @param tag           allocation tag, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      When multiple tags specified. Returns cardinality
   *                      depends on op. If a specified tag doesn't exist,
   *                      0 will be its cardinality.
   *                      When null/empty tags specified, all tags
   *                      (of the node/app) will be considered.
   * @return cardinality of specified query on the node.
   * @throws InvalidAllocationTagsQueryException when illegal query
   *                                            parameter specified
   */
  public long getNodeCardinality(NodeId nodeId, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    readLock.lock();

    try {
      if (nodeId == null) {
        throw new InvalidAllocationTagsQueryException(
            "Must specify nodeId/tags/op to query cardinality");
      }

      NodeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppMappings.get(applicationId);
      } else{
        mapping = globalMapping;
      }

      if (mapping == null) {
        return 0;
      }

      return mapping.getCardinality(nodeId, tag);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Check if given tag exists on node.
   *
   * @param nodeId        nodeId, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all nodes.
   * @param tag           allocation tag, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      When multiple tags specified. Returns cardinality
   *                      depends on op. If a specified tag doesn't exist,
   *                      0 will be its cardinality.
   *                      When null/empty tags specified, all tags
   *                      (of the node/app) will be considered.
   * @return cardinality of specified query on the node.
   * @throws InvalidAllocationTagsQueryException when illegal query
   *                                            parameter specified
   */
  public boolean allocationTagExistsOnNode(NodeId nodeId,
      ApplicationId applicationId, String tag)
      throws InvalidAllocationTagsQueryException {
    return getNodeCardinality(nodeId, applicationId, tag) > 0;
  }

  /**
   * Get cardinality for following conditions. External can pass-in a binary op
   * to implement customized logic.
   *
   * @param nodeId        nodeId, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all nodes.
   * @param tags          allocation tags, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      When multiple tags specified. Returns cardinality
   *                      depends on op. If a specified tag doesn't exist, 0
   *                      will be its cardinality. When null/empty tags
   *                      specified, all tags (of the node/app) will be
   *                      considered.
   * @param op            operator. Such as Long::max, Long::sum, etc. Required.
   *                      This sparameter only take effect when #values >= 2.
   * @return cardinality of specified query on the node.
   * @throws InvalidAllocationTagsQueryException when illegal query
   *                                            parameter specified
   */
  public long getNodeCardinalityByOp(NodeId nodeId, ApplicationId applicationId,
      Set<String> tags, LongBinaryOperator op)
      throws InvalidAllocationTagsQueryException {
    readLock.lock();

    try {
      if (nodeId == null || op == null) {
        throw new InvalidAllocationTagsQueryException(
            "Must specify nodeId/tags/op to query cardinality");
      }

      NodeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppMappings.get(applicationId);
      } else{
        mapping = globalMapping;
      }

      if (mapping == null) {
        return 0;
      }

      return mapping.getCardinality(nodeId, tags, op);
    } finally {
      readLock.unlock();
    }
  }
}
