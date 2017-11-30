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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;

import java.util.Map;

public class RMAppMetrics {
  final Resource resourcePreempted;
  final int numNonAMContainersPreempted;
  final int numAMContainersPreempted;
  private final Map<String, Long> resourceSecondsMap;
  private final Map<String, Long> preemptedResourceSecondsMap;

  public RMAppMetrics(Resource resourcePreempted,
      int numNonAMContainersPreempted, int numAMContainersPreempted,
      Map<String, Long> resourceSecondsMap,
      Map<String, Long> preemptedResourceSecondsMap) {
    this.resourcePreempted = resourcePreempted;
    this.numNonAMContainersPreempted = numNonAMContainersPreempted;
    this.numAMContainersPreempted = numAMContainersPreempted;
    this.resourceSecondsMap = resourceSecondsMap;
    this.preemptedResourceSecondsMap = preemptedResourceSecondsMap;
  }

  public Resource getResourcePreempted() {
    return resourcePreempted;
  }

  public int getNumNonAMContainersPreempted() {
    return numNonAMContainersPreempted;
  }

  public int getNumAMContainersPreempted() {
    return numAMContainersPreempted;
  }

  public long getMemorySeconds() {
    return RMServerUtils.getOrDefault(resourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  public long getVcoreSeconds() {
    return RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L);
  }

  public long getPreemptedMemorySeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  public long getPreemptedVcoreSeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.VCORES.getName(), 0L);
  }

  public Map<String, Long> getResourceSecondsMap() {
    return resourceSecondsMap;
  }

  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return preemptedResourceSecondsMap;
  }

}
