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
  private final Map<String, Long> guaranteedResourceSecondsMap;
  private final Map<String, Long> preemptedResourceSecondsMap;
  private final Map<String, Long> opportunisticResourceSecondsMap;

  public RMAppMetrics(Resource resourcePreempted,
      int numNonAMContainersPreempted, int numAMContainersPreempted,
      Map<String, Long> guaranteedResourceSecondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      Map<String, Long> opportunisticResourceSecondsMap) {
    this.resourcePreempted = resourcePreempted;
    this.numNonAMContainersPreempted = numNonAMContainersPreempted;
    this.numAMContainersPreempted = numAMContainersPreempted;
    this.guaranteedResourceSecondsMap = guaranteedResourceSecondsMap;
    this.preemptedResourceSecondsMap = preemptedResourceSecondsMap;
    this.opportunisticResourceSecondsMap = opportunisticResourceSecondsMap;
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

  public long getGuaranteedMemorySeconds() {
    return RMServerUtils.getOrDefault(guaranteedResourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  public long getGuaranteedVcoreSeconds() {
    return RMServerUtils.getOrDefault(guaranteedResourceSecondsMap,
        ResourceInformation.VCORES.getName(), 0L);
  }

  public long getOpportunisticMemorySeconds() {
    return RMServerUtils.getOrDefault(opportunisticResourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  public long getOpportunisticVcoreSeconds() {
    return RMServerUtils.getOrDefault(opportunisticResourceSecondsMap,
        ResourceInformation.VCORES.getName(), 0L);
  }
  public long getPreemptedMemorySeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  public long getPreemptedVcoreSeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.VCORES.getName(), 0L);
  }

  public Map<String, Long> getGuaranteedResourceSecondsMap() {
    return guaranteedResourceSecondsMap;
  }

  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return preemptedResourceSecondsMap;
  }

  public Map<String, Long> getOpportunisticResourceSecondsMap() {
    return opportunisticResourceSecondsMap;
  }
}
