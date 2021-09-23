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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.collections.map.LazyMap;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.HashMap;
import java.util.Map;

/**
 * A storage that encapsulates intermediate calculation values throughout a
 * full queue update phase.
 */
public class QueueHierarchyUpdateContext {
  private final Resource updatedClusterResource;

  private final Map<String, QueueBranchContext> queueBranchContext
      = LazyMap.decorate(new HashMap<String, QueueBranchContext>(),
      QueueBranchContext::new);
  private Map<String, Map<String, ResourceVector>> normalizedResourceRatios =
      createLazyResourceVector();
  private Map<String, Map<String, ResourceVector>> relativeResourceRatio =
      createLazyResourceVector();

  public QueueHierarchyUpdateContext(
      Resource updatedClusterResource,
      QueueHierarchyUpdateContext queueHierarchyUpdateContext) {
    this.updatedClusterResource = updatedClusterResource;
    this.normalizedResourceRatios = queueHierarchyUpdateContext
        .normalizedResourceRatios;
    this.relativeResourceRatio = queueHierarchyUpdateContext
        .relativeResourceRatio;
  }

  public QueueHierarchyUpdateContext(Resource updatedClusterResource) {
    this.updatedClusterResource = updatedClusterResource;
  }

  private static Map<String, Map<String, ResourceVector>>
  createLazyResourceVector() {
    return LazyMap.decorate(
        new HashMap<String, Map<String, ResourceVector>>(),
        () -> LazyMap.decorate(
            new HashMap<String, ResourceVector>(),
            ResourceVector::newInstance));
  }

  /**
   * Returns the overall cluster resource available for the update phase.
   * @return cluster resource
   */
  public Resource getUpdatedClusterResource() {
    return updatedClusterResource;
  }

  /**
   * Returns the context for a queue branch, which is identified by the path of
   * the parent.
   * @param queuePath queue path of the parent
   * @return queue branch context
   */
  public QueueBranchContext getQueueBranchContext(String queuePath) {
    return queueBranchContext.get(queuePath);
  }

  /**
   * Returns the normalized resource ratio calculated for a queue.
   * @param queuePath queue path
   * @param label node label
   * @return normalized resource ratio
   */
  public ResourceVector getNormalizedMinResourceRatio(
      String queuePath, String label) {
    return normalizedResourceRatios.get(queuePath).get(label);
  }

  /**
   * Returns the ratio of a child queue and its parent's resource.
   * @param queuePath queue path
   * @param label node label
   * @return resource ratio
   */
  public ResourceVector getRelativeResourceRatio(
      String queuePath, String label) {
    return relativeResourceRatio.get(queuePath).get(label);
  }
}
