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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/*
 * Contains the state data that needs to be persisted for an ApplicationAttempt
 */
@Public
@Unstable
public abstract class ApplicationAttemptStateData {

  public static ApplicationAttemptStateData newInstance(
      ApplicationAttemptId attemptId, Container container,
      Credentials attemptTokens, long startTime, RMAppAttemptState finalState,
      String finalTrackingUrl, String diagnostics,
      FinalApplicationStatus amUnregisteredFinalStatus, int exitStatus,
      long finishTime, Map<String, Long> resourceSecondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      int totalAllocatedContainers) {
    ApplicationAttemptStateData attemptStateData =
        Records.newRecord(ApplicationAttemptStateData.class);
    attemptStateData.setAttemptId(attemptId);
    attemptStateData.setMasterContainer(container);
    attemptStateData.setAppAttemptTokens(attemptTokens);
    attemptStateData.setState(finalState);
    attemptStateData.setFinalTrackingUrl(finalTrackingUrl);
    attemptStateData.setDiagnostics(diagnostics == null ? "" : diagnostics);
    attemptStateData.setStartTime(startTime);
    attemptStateData.setFinalApplicationStatus(amUnregisteredFinalStatus);
    attemptStateData.setAMContainerExitStatus(exitStatus);
    attemptStateData.setFinishTime(finishTime);
    attemptStateData.setMemorySeconds(RMServerUtils
        .getOrDefault(resourceSecondsMap,
            ResourceInformation.MEMORY_MB.getName(), 0L));
    attemptStateData.setVcoreSeconds(RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L));
    attemptStateData.setPreemptedMemorySeconds(RMServerUtils
        .getOrDefault(preemptedResourceSecondsMap,
            ResourceInformation.MEMORY_MB.getName(), 0L));
    attemptStateData.setPreemptedVcoreSeconds(RMServerUtils
        .getOrDefault(preemptedResourceSecondsMap,
            ResourceInformation.VCORES.getName(), 0L));
    attemptStateData.setResourceSecondsMap(resourceSecondsMap);
    attemptStateData
        .setPreemptedResourceSecondsMap(preemptedResourceSecondsMap);
    attemptStateData.setTotalAllocatedContainers(totalAllocatedContainers);
    return attemptStateData;
  }

  public static ApplicationAttemptStateData newInstance(
      ApplicationAttemptId attemptId, Container masterContainer,
      Credentials attemptTokens, long startTime,
      Map<String, Long> resourceSeondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      int totalAllocatedContainers) {
    return newInstance(attemptId, masterContainer, attemptTokens, startTime,
        null, "N/A", "", null, ContainerExitStatus.INVALID, 0,
        resourceSeondsMap, preemptedResourceSecondsMap,
        totalAllocatedContainers);
  }


  public abstract ApplicationAttemptStateDataProto getProto();

  /**
   * The ApplicationAttemptId for the application attempt
   * @return ApplicationAttemptId for the application attempt
   */
  @Public
  @Unstable
  public abstract ApplicationAttemptId getAttemptId();
  
  public abstract void setAttemptId(ApplicationAttemptId attemptId);
  
  /*
   * The master container running the application attempt
   * @return Container that hosts the attempt
   */
  @Public
  @Unstable
  public abstract Container getMasterContainer();
  
  public abstract void setMasterContainer(Container container);

  /**
   * The application attempt tokens that belong to this attempt
   * @return The application attempt tokens that belong to this attempt
   */
  @Public
  @Unstable
  public abstract Credentials getAppAttemptTokens();

  public abstract void setAppAttemptTokens(Credentials attemptTokens);

  /**
   * Get the final state of the application attempt.
   * @return the final state of the application attempt.
   */
  public abstract RMAppAttemptState getState();

  public abstract void setState(RMAppAttemptState state);

  /**
   * Get the original not-proxied <em>final tracking url</em> for the
   * application. This is intended to only be used by the proxy itself.
   * 
   * @return the original not-proxied <em>final tracking url</em> for the
   *         application
   */
  public abstract String getFinalTrackingUrl();

  /**
   * Set the final tracking Url of the AM.
   * @param url
   */
  public abstract void setFinalTrackingUrl(String url);
  /**
   * Get the <em>diagnositic information</em> of the attempt 
   * @return <em>diagnositic information</em> of the attempt
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  public abstract long getStartTime();

  public abstract void setStartTime(long startTime);

  /**
   * Get the <em>final finish status</em> of the application.
   * @return <em>final finish status</em> of the application
   */
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  public abstract void setFinalApplicationStatus(
      FinalApplicationStatus finishState);

  public abstract int getAMContainerExitStatus();

  public abstract void setAMContainerExitStatus(int exitStatus);

  /**
   * Get the <em>finish time</em> of the application attempt.
   * @return <em>finish time</em> of the application attempt
   */
  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);

  /**
  * Get the <em>memory seconds</em> (in MB seconds) of the application.
   * @return <em>memory seconds</em> (in MB seconds) of the application
   */
  @Public
  @Unstable
  public abstract long getMemorySeconds();

  @Public
  @Unstable
  public abstract void setMemorySeconds(long memorySeconds);

  /**
   * Get the <em>vcore seconds</em> of the application.
   * @return <em>vcore seconds</em> of the application
   */
  @Public
  @Unstable
  public abstract long getVcoreSeconds();

  @Public
  @Unstable
  public abstract void setVcoreSeconds(long vcoreSeconds);

  /**
   * Get the <em>preempted memory seconds</em>
   * (in MB seconds) of the application.
   * @return <em>preempted memory seconds</em>
   * (in MB seconds) of the application
   */
  @Public
  @Unstable
  public abstract long getPreemptedMemorySeconds();

  @Public
  @Unstable
  public abstract void setPreemptedMemorySeconds(long memorySeconds);

  /**
   * Get the <em>preempted vcore seconds</em>
   * of the application.
   * @return <em>preempted vcore seconds</em>
   * of the application
   */
  @Public
  @Unstable
  public abstract long getPreemptedVcoreSeconds();

  @Public
  @Unstable
  public abstract void setPreemptedVcoreSeconds(long vcoreSeconds);

  /**
   * Get the aggregated number of resources preempted that the application has
   * allocated times the number of seconds the application has been running.
   *
   * @return map containing the resource name and aggregated preempted
   * resource-seconds
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getResourceSecondsMap();

  /**
   * Set the aggregated number of resources that the application has
   * allocated times the number of seconds the application has been running.
   *
   * @param resourceSecondsMap map containing the resource name and aggregated
   *                           resource-seconds
   */
  @Public
  @Unstable
  public abstract void setResourceSecondsMap(
      Map<String, Long> resourceSecondsMap);

  /**
   * Get the aggregated number of resources preempted that the application has
   * allocated times the number of seconds the application has been running.
   *
   * @return map containing the resource name and aggregated preempted
   * resource-seconds
   */
  @Public
  @Unstable
  public abstract Map<String, Long> getPreemptedResourceSecondsMap();

  /**
   * Set the aggregated number of resources preempted that the application has
   * allocated times the number of seconds the application has been running.
   *
   * @param preemptedResourceSecondsMap map containing the resource name and
   *                                    aggregated preempted resource-seconds
   */
  @Public
  @Unstable
  public abstract void setPreemptedResourceSecondsMap(
      Map<String, Long> preemptedResourceSecondsMap);

  /**
   * Get total number of containers allocated for this attempt.
   *
   * @return total number of containers allocated for this attempt.
   */
  @Public
  @Unstable
  public abstract int getTotalAllocatedContainers();

  /**
   * Set total number of containers allocated for this attempt.
   *
   * @param totalAllocatedContainers total number of containers
   */
  @Public
  @Unstable
  public abstract void setTotalAllocatedContainers(
      int totalAllocatedContainers);

}
