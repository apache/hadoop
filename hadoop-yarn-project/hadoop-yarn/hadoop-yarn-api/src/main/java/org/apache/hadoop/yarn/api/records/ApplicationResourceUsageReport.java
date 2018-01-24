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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Contains various scheduling metrics to be reported by UI and CLI.
 */
@Public
@Stable
public abstract class ApplicationResourceUsageReport {

  @Private
  @Unstable
  public static ApplicationResourceUsageReport newInstance(
      int numUsedContainers, int numReservedContainers,
      Resource guaranteedResourcesUsed,
      Resource reservedResources, Resource neededResources,
      Map<String, Long> guaranteedResourceSecondsMap, float queueUsagePerc,
      float clusterUsagePerc, Map<String, Long> preemtedResourceSecondsMap,
      Resource opportunisticResourcesUsed,
      Map<String, Long> opportunisticResourcesSecondsMap) {

    ApplicationResourceUsageReport report =
        Records.newRecord(ApplicationResourceUsageReport.class);
    report.setNumUsedContainers(numUsedContainers);
    report.setNumReservedContainers(numReservedContainers);
    report.setGuaranteedResourcesUsed(guaranteedResourcesUsed);
    report.setReservedResources(reservedResources);
    report.setNeededResources(neededResources);
    report.setGuaranteedResourceSecondsMap(guaranteedResourceSecondsMap);
    report.setQueueUsagePercentage(queueUsagePerc);
    report.setClusterUsagePercentage(clusterUsagePerc);
    report.setPreemptedResourceSecondsMap(preemtedResourceSecondsMap);
    report.setOpportunisticResourcesUsed(opportunisticResourcesUsed);
    report.setOpportunisticResourceSecondsMap(opportunisticResourcesSecondsMap);
    return report;
  }

  /**
   * Get the number of used containers.  -1 for invalid/inaccessible reports.
   * @return the number of used containers
   */
  @Public
  @Stable
  public abstract int getNumUsedContainers();

  /**
   * Set the number of used containers
   * @param num_containers the number of used containers
   */
  @Private
  @Unstable
  public abstract void setNumUsedContainers(int num_containers);

  /**
   * Get the number of reserved containers.  -1 for invalid/inaccessible reports.
   * @return the number of reserved containers
   */
  @Private
  @Unstable
  public abstract int getNumReservedContainers();

  /**
   * Set the number of reserved containers
   * @param num_reserved_containers the number of reserved containers
   */
  @Private
  @Unstable
  public abstract void setNumReservedContainers(int num_reserved_containers);

  /**
   * Get the guaranteed <code>Resource</code> used.
   * -1 for invalid/inaccessible reports.
   * @return the guaranteed <code>Resource</code> used
   */
  @Public
  @Stable
  @Deprecated
  public abstract Resource getUsedResources();

  /**
   * Get the guaranteed <code>Resource</code> used.
   * -1 for invalid/inaccessible reports.
   * @return the guaranteed <code>Resource</code> used
   */
  @Public
  @Unstable
  public abstract Resource getGuaranteedResourcesUsed();

  @Private
  @Unstable
  public abstract void setGuaranteedResourcesUsed(Resource resources);

  /**
   * Get the opportunistic <code>Resource</code> used.
   * -1 for invalid/inaccessible reports.
   * @return the opportunistic <code>Resource</code> used
   */
  @Public
  @Unstable
  public abstract Resource getOpportunisticResourcesUsed();

  @Private
  @Unstable
  public abstract void setOpportunisticResourcesUsed(Resource resources);

  /**
   * Get the reserved <code>Resource</code>.  -1 for invalid/inaccessible reports.
   * @return the reserved <code>Resource</code>
   */
  @Public
  @Stable
  public abstract Resource getReservedResources();

  @Private
  @Unstable
  public abstract void setReservedResources(Resource reserved_resources);

  /**
   * Get the needed <code>Resource</code>.  -1 for invalid/inaccessible reports.
   * @return the needed <code>Resource</code>
   */
  @Public
  @Stable
  public abstract Resource getNeededResources();

  @Private
  @Unstable
  public abstract void setNeededResources(Resource needed_resources);

  /**
   * Set the aggregated amount of guaranteed memory (in megabytes) the
   * application has allocated times the number of seconds the application
   * has been running.
   * @param memorySeconds the aggregated amount of guaranteed memory seconds
   */
  @Private
  @Unstable
  public abstract void setGuaranteedMemorySeconds(long memorySeconds);

  /**
   * Get the aggregated amount of guaranteed memory (in megabytes) the
   * application has allocated times the number of seconds the application
   * has been running.
   * @return the aggregated amount of guaranteed memory seconds
   */
  @Public
  @Unstable
  public abstract long getGuaranteedMemorySeconds();

  /**
   * Get the aggregated amount of guaranteed memory (in megabytes) the
   * application has allocated times the number of seconds the application
   * has been running.
   * @return the aggregated amount of guaranteed memory seconds
   */
  @Public
  @Unstable
  @Deprecated
  public abstract long getMemorySeconds();

  /**
   * Set the aggregated number of guaranteed vcores that the application has
   * allocated times the number of seconds the application has been running.
   * @param vcoreSeconds the aggregated number of guaranteed vcore seconds
   */
  @Private
  @Unstable
  public abstract void setGuaranteedVcoreSeconds(long vcoreSeconds);

  /**
   * Get the aggregated number of guaranteed vcores that the application has
   * allocated times the number of seconds the application has been running.
   * @return the aggregated number of guaranteed vcore seconds
   */
  @Public
  @Unstable
  public abstract long getGuaranteedVcoreSeconds();

  /**
   * Get the aggregated number of guaranteed vcores that the application has
   * allocated times the number of seconds the application has been running.
   * @return the aggregated number of guaranteed vcore seconds
   */
  @Public
  @Unstable
  @Deprecated
  public abstract long getVcoreSeconds();

  /**
   * Get the aggregated amount of opportunistic memory (in megabytes) the
   * application has allocated times the number of seconds the application
   * has been running.
   * @return the aggregated amount of opportunistic memory seconds
   */
  @Public
  @Unstable
  public abstract long getOpportunisticMemorySeconds();

  /**
   * Get the aggregated number of opportunistic vcores that the application
   * has allocated times the number of seconds the application has been running.
   * @return the aggregated number of opportunistic vcore seconds
   */
  @Public
  @Unstable
  public abstract long getOpportunisticVcoreSeconds();

  /**
   * Get the percentage of resources of the queue that the app is using.
   * @return the percentage of resources of the queue that the app is using.
   */
  @Public
  @Stable
  public abstract float getQueueUsagePercentage();

  /**
   * Set the percentage of resources of the queue that the app is using.
   * @param queueUsagePerc the percentage of resources of the queue that
   *                       the app is using.
   */
  @Private
  @Unstable
  public abstract void setQueueUsagePercentage(float queueUsagePerc);

  /**
   * Get the percentage of resources of the cluster that the app is using.
   * @return the percentage of resources of the cluster that the app is using.
   */
  @Public
  @Stable
  public abstract float getClusterUsagePercentage();

  /**
   * Set the percentage of resources of the cluster that the app is using.
   * @param clusterUsagePerc the percentage of resources of the cluster that
   *                         the app is using.
   */
  @Private
  @Unstable
  public abstract void setClusterUsagePercentage(float clusterUsagePerc);

  /**
   * Set the aggregated amount of memory preempted (in megabytes)
   * the application has allocated times the number of seconds
   * the application has been running.
   * @param memorySeconds the aggregated amount of memory seconds
   */
  @Private
  @Unstable
  public abstract void setPreemptedMemorySeconds(long memorySeconds);

  /**
   * Get the aggregated amount of memory preempted(in megabytes)
   * the application has allocated times the number of
   * seconds the application has been running.
   * @return the aggregated amount of memory seconds
   */
  @Public
  @Unstable
  public abstract long getPreemptedMemorySeconds();

  /**
   * Set the aggregated number of vcores preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @param vcoreSeconds the aggregated number of vcore seconds
   */
  @Private
  @Unstable
  public abstract void setPreemptedVcoreSeconds(long vcoreSeconds);

  /**
   * Get the aggregated number of vcores preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @return the aggregated number of vcore seconds
   */
  @Public
  @Unstable
  public abstract long getPreemptedVcoreSeconds();

  /**
   * Get the aggregated number of guaranteed resources that the application has
   * allocated times the number of seconds the application has been running.
   * @return map containing the resource name and aggregated guaranteed
   *         resource-seconds
   */
  @Public
  @Unstable
  @Deprecated
  public abstract Map<String, Long> getResourceSecondsMap();

  /**
   * Get the aggregated number of guaranteed resources that the application has
   * allocated times the number of seconds the application has been running.
   * @return map containing the resource name and aggregated guaranteed
   *         resource-seconds
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getGuaranteedResourceSecondsMap();

  /**
   * Set the aggregated number of guaranteed resources that the application has
   * allocated times the number of seconds the application has been running.
   * @param resourceSecondsMap map containing the resource name and aggregated
   *                           guaranteed resource-seconds
   */
  @Private
  @Unstable
  public abstract void setGuaranteedResourceSecondsMap(
      Map<String, Long> resourceSecondsMap);


  /**
   * Get the aggregated number of resources preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @return map containing the resource name and aggregated preempted
   * resource-seconds
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getPreemptedResourceSecondsMap();

  /**
   * Set the aggregated number of resources preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @param preemptedResourceSecondsMap  map containing the resource name and
   *                                     aggregated preempted resource-seconds
   */
  @Private
  @Unstable
  public abstract void setPreemptedResourceSecondsMap(
      Map<String, Long> preemptedResourceSecondsMap);

  /**
   * Get the aggregated number of opportunistic resources that the application
   * has allocated times the number of seconds the application has been running.
   * @return map containing the resource name and aggregated opportunistic
   *         resource-seconds
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getOpportunisticResourceSecondsMap();

  /**
   * Set the aggregated number of opportunistic resources that the application
   * has allocated times the number of seconds the application has been running.
   * @param opportunisticResourceSecondsMap map containing the resource name
   *                               and aggregated opportunistic resource-seconds
   */
  @Private
  @Unstable
  public abstract void setOpportunisticResourceSecondsMap(
      Map<String, Long> opportunisticResourceSecondsMap);
}
