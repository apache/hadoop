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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongBinaryOperator;

/**
 * In-memory mapping between applications/container-tags and nodes/racks.
 * Required by constrained affinity/anti-affinity and cardinality placement.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AllocationTagsManager {

  private static final Logger LOG = Logger.getLogger(
      AllocationTagsManager.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;
  private final RMContext rmContext;

  // Application's tags to Node
  private Map<ApplicationId, TypeToCountedTags> perAppNodeMappings =
      new HashMap<>();
  // Application's tags to Rack
  private Map<ApplicationId, TypeToCountedTags> perAppRackMappings =
      new HashMap<>();

  // Global tags to node mapping (used to fast return aggregated tags
  // cardinality across apps)
  private TypeToCountedTags<NodeId> globalNodeMapping = new TypeToCountedTags();
  // Global tags to Rack mapping
  private TypeToCountedTags<String> globalRackMapping = new TypeToCountedTags();

  /**
   * Generic store mapping type T to counted tags.
   * Currently used both for NodeId to Tag, Count and Rack to Tag, Count
   */
  @VisibleForTesting
  public static class TypeToCountedTags<T> {
    // Map<Type, Map<Tag, Count>>
    private Map<T, Map<String, Long>> typeToTagsWithCount = new HashMap<>();

    // protected by external locks
    private void addTags(T type, Set<String> tags) {
      Map<String, Long> innerMap =
          typeToTagsWithCount.computeIfAbsent(type, k -> new HashMap<>());

      for (String tag : tags) {
        Long count = innerMap.get(tag);
        if (count == null) {
          innerMap.put(tag, 1L);
        } else {
          innerMap.put(tag, count + 1);
        }
      }
    }

    // protected by external locks
    private void addTag(T type, String tag) {
      Map<String, Long> innerMap =
          typeToTagsWithCount.computeIfAbsent(type, k -> new HashMap<>());

      Long count = innerMap.get(tag);
      if (count == null) {
        innerMap.put(tag, 1L);
      } else {
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
              "Trying to remove tags from node/rack, however the count already"
                  + " becomes 0 or less, it could be a potential bug.");
        }
        innerMap.remove(tag);
      }
    }

    private void removeTags(T type, Set<String> tags) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        LOG.warn("Failed to find node/rack=" + type
            + " while trying to remove tags, please double check.");
        return;
      }

      for (String tag : tags) {
        removeTagFromInnerMap(innerMap, tag);
      }

      if (innerMap.isEmpty()) {
        typeToTagsWithCount.remove(type);
      }
    }

    private void removeTag(T type, String tag) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        LOG.warn("Failed to find node/rack=" + type
            + " while trying to remove tags, please double check.");
        return;
      }

      removeTagFromInnerMap(innerMap, tag);

      if (innerMap.isEmpty()) {
        typeToTagsWithCount.remove(type);
      }
    }

    private long getCardinality(T type, String tag) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
      if (innerMap == null) {
        return 0;
      }
      Long value = innerMap.get(tag);
      return value == null ? 0 : value;
    }

    private long getCardinality(T type, Set<String> tags,
        LongBinaryOperator op) {
      Map<String, Long> innerMap = typeToTagsWithCount.get(type);
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
      return typeToTagsWithCount.isEmpty();
    }

    @VisibleForTesting
    public Map<T, Map<String, Long>> getTypeToTagsWithCount() {
      return typeToTagsWithCount;
    }
  }

  @VisibleForTesting
  public Map<ApplicationId, TypeToCountedTags> getPerAppNodeMappings() {
    return perAppNodeMappings;
  }

  @VisibleForTesting
  Map<ApplicationId, TypeToCountedTags> getPerAppRackMappings() {
    return perAppRackMappings;
  }

  @VisibleForTesting
  TypeToCountedTags getGlobalNodeMapping() {
    return globalNodeMapping;
  }

  @VisibleForTesting
  TypeToCountedTags getGlobalRackMapping() {
    return globalRackMapping;
  }

  public AllocationTagsManager(RMContext context) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    rmContext = context;
  }

  /**
   * Notify container allocated on a node.
   *
   * @param nodeId         allocated node.
   * @param containerId    container id.
   * @param allocationTags allocation tags, see
   *                       {@link SchedulingRequest#getAllocationTags()}
   *                       application_id will be added to allocationTags.
   */
  @SuppressWarnings("unchecked")
  public void addContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    // Do nothing for empty allocation tags.
    if (allocationTags == null || allocationTags.isEmpty()) {
      return;
    }
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();
    addTags(nodeId, applicationId, allocationTags);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added container=" + containerId + " with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
  }

  public void addTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    writeLock.lock();
    try {
      TypeToCountedTags perAppTagsMapping = perAppNodeMappings
          .computeIfAbsent(applicationId, k -> new TypeToCountedTags());
      TypeToCountedTags perAppRackTagsMapping = perAppRackMappings
          .computeIfAbsent(applicationId, k -> new TypeToCountedTags());
      // Covering test-cases where context is mocked
      String nodeRack = (rmContext.getRMNodes() != null
          && rmContext.getRMNodes().get(nodeId) != null)
              ? rmContext.getRMNodes().get(nodeId).getRackName() :
          "default-rack";
      perAppTagsMapping.addTags(nodeId, allocationTags);
      perAppRackTagsMapping.addTags(nodeRack, allocationTags);
      globalNodeMapping.addTags(nodeId, allocationTags);
      globalRackMapping.addTags(nodeRack, allocationTags);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Notify container removed.
   *
   * @param nodeId         nodeId
   * @param containerId    containerId.
   * @param allocationTags allocation tags for given container
   */
  @SuppressWarnings("unchecked")
  public void removeContainer(NodeId nodeId,
      ContainerId containerId, Set<String> allocationTags) {
    // Do nothing for empty allocation tags.
    if (allocationTags == null || allocationTags.isEmpty()) {
      return;
    }
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();

    removeTags(nodeId, applicationId, allocationTags);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed container=" + containerId + " with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
  }

  /**
   * Helper method to just remove the tags associated with a container.
   * @param nodeId
   * @param applicationId
   * @param allocationTags
   */
  public void removeTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    writeLock.lock();
    try {
      TypeToCountedTags perAppTagsMapping =
          perAppNodeMappings.get(applicationId);
      TypeToCountedTags perAppRackTagsMapping =
          perAppRackMappings.get(applicationId);
      if (perAppTagsMapping == null) {
        return;
      }
      // Covering test-cases where context is mocked
      String nodeRack = (rmContext.getRMNodes() != null
          && rmContext.getRMNodes().get(nodeId) != null)
              ? rmContext.getRMNodes().get(nodeId).getRackName() :
          "default-rack";
      perAppTagsMapping.removeTags(nodeId, allocationTags);
      perAppRackTagsMapping.removeTags(nodeRack, allocationTags);
      globalNodeMapping.removeTags(nodeId, allocationTags);
      globalRackMapping.removeTags(nodeRack, allocationTags);

      if (perAppTagsMapping.isEmpty()) {
        perAppNodeMappings.remove(applicationId);
      }
      if (perAppRackTagsMapping.isEmpty()) {
        perAppRackMappings.remove(applicationId);
      }
    } finally {
      writeLock.unlock();
    }
  }


  /**
   * Get Node cardinality for a specific tag.
   * When applicationId is null, method returns aggregated cardinality
   *
   * @param nodeId        nodeId, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all nodes.
   * @param tag           allocation tag, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      If a specified tag doesn't exist,
   *                      method returns 0.
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
            "Must specify nodeId/tag to query cardinality");
      }

      TypeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppNodeMappings.get(applicationId);
      } else {
        mapping = globalNodeMapping;
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
   * Get Rack cardinality for a specific tag.
   *
   * @param rack          rack, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all nodes.
   * @param tag           allocation tag, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      If a specified tag doesn't exist,
   *                      method returns 0.
   * @return cardinality of specified query on the rack.
   * @throws InvalidAllocationTagsQueryException when illegal query
   *                                            parameter specified
   */
  public long getRackCardinality(String rack, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    readLock.lock();

    try {
      if (rack == null) {
        throw new InvalidAllocationTagsQueryException(
            "Must specify rack/tag to query cardinality");
      }

      TypeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppRackMappings.get(applicationId);
      } else {
        mapping = globalRackMapping;
      }

      if (mapping == null) {
        return 0;
      }

      return mapping.getCardinality(rack, tag);
    } finally {
      readLock.unlock();
    }
  }



  /**
   * Check if given tag exists on node.
   *
   * @param nodeId        nodeId, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregation among all applications.
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
   *                      aggregated cardinality among all applications.
   * @param tags          allocation tags, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      When multiple tags specified. Returns cardinality
   *                      depends on op. If a specified tag doesn't exist, 0
   *                      will be its cardinality. When null/empty tags
   *                      specified, all tags (of the node/app) will be
   *                      considered.
   * @param op            operator. Such as Long::max, Long::sum, etc. Required.
   *                      This parameter only take effect when #values greater
   *                      than 2.
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

      TypeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppNodeMappings.get(applicationId);
      } else {
        mapping = globalNodeMapping;
      }

      if (mapping == null) {
        return 0;
      }

      return mapping.getCardinality(nodeId, tags, op);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get cardinality for following conditions. External can pass-in a binary op
   * to implement customized logic.
   *
   * @param rack          rack, required.
   * @param applicationId applicationId. When null is specified, return
   *                      aggregated cardinality among all applications.
   * @param tags          allocation tags, see
   *                      {@link SchedulingRequest#getAllocationTags()},
   *                      When multiple tags specified. Returns cardinality
   *                      depends on op. If a specified tag doesn't exist, 0
   *                      will be its cardinality. When null/empty tags
   *                      specified, all tags (of the rack/app) will be
   *                      considered.
   * @param op            operator. Such as Long::max, Long::sum, etc. Required.
   *                      This parameter only take effect when #values
   *                      greater than 2.
   * @return cardinality of specified query on the rack.
   * @throws InvalidAllocationTagsQueryException when illegal query
   *                                            parameter specified
   */
  @SuppressWarnings("unchecked")
  public long getRackCardinalityByOp(String rack, ApplicationId applicationId,
      Set<String> tags, LongBinaryOperator op)
      throws InvalidAllocationTagsQueryException {
    readLock.lock();

    try {
      if (rack == null || op == null) {
        throw new InvalidAllocationTagsQueryException(
            "Must specify rack/tags/op to query cardinality");
      }

      TypeToCountedTags mapping;
      if (applicationId != null) {
        mapping = perAppRackMappings.get(applicationId);
      } else {
        mapping = globalRackMapping;
      }

      if (mapping == null) {
        return 0;
      }

      return mapping.getCardinality(rack, tags, op);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns a map whose key is the allocation tag and value is the
   * count of allocations with this tag.
   *
   * @param nodeId
   * @return allocation tag to count mapping
   */
  public Map<String, Long> getAllocationTagsWithCount(NodeId nodeId) {
    return globalNodeMapping.getTypeToTagsWithCount().get(nodeId);
  }
}
