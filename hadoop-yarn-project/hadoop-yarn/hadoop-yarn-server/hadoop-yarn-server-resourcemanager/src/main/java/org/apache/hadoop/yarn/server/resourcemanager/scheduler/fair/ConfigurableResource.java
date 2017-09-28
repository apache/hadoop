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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A {@code ConfigurableResource} object represents an entity that is used to
 * configure resources, such as maximum resources of a queue. It can be
 * percentage of cluster resources or an absolute value.
 */
@Private
@Unstable
public class ConfigurableResource {
  private final Resource resource;
  private final double[] percentages;

  public ConfigurableResource(double[] percentages) {
    this.percentages = percentages.clone();
    this.resource = null;
  }

  public ConfigurableResource(Resource resource) {
    this.percentages = null;
    this.resource = resource;
  }

  /**
   * Get resource by multiplying the cluster resource and the percentage of
   * each resource respectively. Return the absolute resource if either
   * {@code percentages} or {@code clusterResource) is null.
   *
   * @param clusterResource the cluster resource
   * @return resource
   */
  public Resource getResource(Resource clusterResource) {
    if (percentages != null && clusterResource != null) {
      long memory = (long) (clusterResource.getMemorySize() * percentages[0]);
      int vcore = (int) (clusterResource.getVirtualCores() * percentages[1]);
      return Resource.newInstance(memory, vcore);
    } else {
      return resource;
    }
  }

  /**
   * Get the absolute resource.
   *
   * @return the absolute resource
   */
  public Resource getResource() {
    return resource;
  }
}
