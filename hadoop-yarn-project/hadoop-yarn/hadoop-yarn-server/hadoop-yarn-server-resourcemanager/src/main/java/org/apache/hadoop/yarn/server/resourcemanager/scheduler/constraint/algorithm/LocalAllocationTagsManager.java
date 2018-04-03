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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTags;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongBinaryOperator;

class LocalAllocationTagsManager extends AllocationTagsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalAllocationTagsManager.class);

  private final AllocationTagsManager tagsManager;

  // Application's Temporary containers mapping
  private Map<ApplicationId, Map<NodeId, Map<String, AtomicInteger>>>
      appTempMappings = new HashMap<>();

  LocalAllocationTagsManager(
      AllocationTagsManager allocationTagsManager) {
    super(null);
    this.tagsManager = allocationTagsManager;
  }

  void addTempTags(NodeId nodeId,
      ApplicationId applicationId, Set<String> allocationTags) {
    Map<NodeId, Map<String, AtomicInteger>> appTempMapping =
        appTempMappings.computeIfAbsent(applicationId, k -> new HashMap<>());
    Map<String, AtomicInteger> containerTempMapping =
        appTempMapping.computeIfAbsent(nodeId, k -> new HashMap<>());
    for (String tag : allocationTags) {
      containerTempMapping.computeIfAbsent(tag,
          k -> new AtomicInteger(0)).incrementAndGet();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added TEMP container with tags=["
          + StringUtils.join(allocationTags, ",") + "]");
    }
    tagsManager.addTags(nodeId, applicationId, allocationTags);
  }

  void removeTempTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    Map<NodeId, Map<String, AtomicInteger>> appTempMapping =
        appTempMappings.get(applicationId);
    if (appTempMapping != null) {
      Map<String, AtomicInteger> containerTempMap =
          appTempMapping.get(nodeId);
      if (containerTempMap != null) {
        for (String tag : allocationTags) {
          AtomicInteger count = containerTempMap.get(tag);
          if (count != null) {
            if (count.decrementAndGet() <= 0) {
              containerTempMap.remove(tag);
            }
          }
        }
      }
    }
    if (allocationTags != null) {
      removeTags(nodeId, applicationId, allocationTags);
    }
  }

  /**
   * Method removes temporary containers associated with an application
   * Used by the placement algorithm to clean temporary tags at the end of
   * a placement cycle.
   * @param applicationId Application Id.
   */
  public void cleanTempContainers(ApplicationId applicationId) {

    if (!appTempMappings.get(applicationId).isEmpty()) {
      appTempMappings.get(applicationId).entrySet().stream().forEach(nodeE -> {
        nodeE.getValue().entrySet().stream().forEach(tagE -> {
          for (int i = 0; i < tagE.getValue().get(); i++) {
            removeTags(nodeE.getKey(), applicationId,
                Collections.singleton(tagE.getKey()));
          }
        });
      });
      appTempMappings.remove(applicationId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removed TEMP containers of app=" + applicationId);
      }
    }
  }

  @Override
  public void addContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    tagsManager.addContainer(nodeId, containerId, allocationTags);
  }

  @Override
  public void removeContainer(NodeId nodeId, ContainerId containerId,
      Set<String> allocationTags) {
    tagsManager.removeContainer(nodeId, containerId, allocationTags);
  }

  @Override
  public void removeTags(NodeId nodeId, ApplicationId applicationId,
      Set<String> allocationTags) {
    tagsManager.removeTags(nodeId, applicationId, allocationTags);
  }

  @Override
  public long getNodeCardinality(NodeId nodeId, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    return tagsManager.getNodeCardinality(nodeId, applicationId, tag);
  }

  @Override
  public long getNodeCardinalityByOp(NodeId nodeId, AllocationTags tags,
      LongBinaryOperator op) throws InvalidAllocationTagsQueryException {
    return tagsManager.getNodeCardinalityByOp(nodeId, tags, op);
  }

  @Override
  public long getRackCardinality(String rack, ApplicationId applicationId,
      String tag) throws InvalidAllocationTagsQueryException {
    return tagsManager.getRackCardinality(rack, applicationId, tag);
  }

  @Override
  public long getRackCardinalityByOp(String rack, AllocationTags tags,
      LongBinaryOperator op) throws InvalidAllocationTagsQueryException {
    return tagsManager.getRackCardinalityByOp(rack, tags, op);
  }

  @Override
  public boolean allocationTagExistsOnNode(NodeId nodeId,
      ApplicationId applicationId, String tag)
      throws InvalidAllocationTagsQueryException {
    return tagsManager.allocationTagExistsOnNode(nodeId, applicationId, tag);
  }
}
