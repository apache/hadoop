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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Priority;

/**
 * Simple class to allow users to send information required to create an
 * ApplicationSubmissionContext which can then be used to submit an app
 * 
 */
@XmlRootElement(name = "application-submission-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class ApplicationSubmissionContextInfo {

  @XmlElement(name = "application-id")
  String applicationId;

  @XmlElement(name = "application-name")
  String applicationName;

  String queue;
  int priority;

  @XmlElement(name = "am-container-spec")
  ContainerLaunchContextInfo containerInfo;

  @XmlElement(name = "unmanaged-AM")
  boolean isUnmanagedAM;

  @XmlElement(name = "cancel-tokens-when-complete")
  boolean cancelTokensWhenComplete;

  @XmlElement(name = "max-app-attempts")
  int maxAppAttempts;

  @XmlElement(name = "resource")
  ResourceInfo resource;

  @XmlElement(name = "application-type")
  String applicationType;

  @XmlElement(name = "keep-containers-across-application-attempts")
  boolean keepContainers;

  @XmlElementWrapper(name = "application-tags")
  @XmlElement(name = "tag")
  Set<String> tags;
  
  @XmlElement(name = "app-node-label-expression")
  String appNodeLabelExpression;
  
  @XmlElement(name = "am-container-node-label-expression")
  String amContainerNodeLabelExpression;

  @XmlElement(name = "log-aggregation-context")
  LogAggregationContextInfo logAggregationContextInfo;

  @XmlElement(name = "attempt-failures-validity-interval")
  long attemptFailuresValidityInterval;

  @XmlElement(name = "reservation-id")
  String reservationId;

  public ApplicationSubmissionContextInfo() {
    applicationId = "";
    applicationName = "";
    containerInfo = new ContainerLaunchContextInfo();
    resource = new ResourceInfo();
    priority = Priority.UNDEFINED.getPriority();
    isUnmanagedAM = false;
    cancelTokensWhenComplete = true;
    keepContainers = false;
    applicationType = "";
    tags = new HashSet<String>();
    appNodeLabelExpression = "";
    amContainerNodeLabelExpression = "";
    logAggregationContextInfo = null;
    attemptFailuresValidityInterval = -1;
    reservationId = "";
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getQueue() {
    return queue;
  }

  public int getPriority() {
    return priority;
  }

  public ContainerLaunchContextInfo getContainerLaunchContextInfo() {
    return containerInfo;
  }

  public boolean getUnmanagedAM() {
    return isUnmanagedAM;
  }

  public boolean getCancelTokensWhenComplete() {
    return cancelTokensWhenComplete;
  }

  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public ResourceInfo getResource() {
    return resource;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public boolean getKeepContainersAcrossApplicationAttempts() {
    return keepContainers;
  }

  public Set<String> getApplicationTags() {
    return tags;
  }
  
  public String getAppNodeLabelExpression() {
    return appNodeLabelExpression;
  }
  
  public String getAMContainerNodeLabelExpression() {
    return amContainerNodeLabelExpression;
  }

  public LogAggregationContextInfo getLogAggregationContextInfo() {
    return logAggregationContextInfo;
  }

  public long getAttemptFailuresValidityInterval() {
    return attemptFailuresValidityInterval;
  }

  public String getReservationId() {
    return reservationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public void setContainerLaunchContextInfo(
      ContainerLaunchContextInfo containerLaunchContext) {
    this.containerInfo = containerLaunchContext;
  }

  public void setUnmanagedAM(boolean isUnmanagedAM) {
    this.isUnmanagedAM = isUnmanagedAM;
  }

  public void setCancelTokensWhenComplete(boolean cancelTokensWhenComplete) {
    this.cancelTokensWhenComplete = cancelTokensWhenComplete;
  }

  public void setMaxAppAttempts(int maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
  }

  public void setResource(ResourceInfo resource) {
    this.resource = resource;
  }

  public void setApplicationType(String applicationType) {
    this.applicationType = applicationType;
  }

  public void
      setKeepContainersAcrossApplicationAttempts(boolean keepContainers) {
    this.keepContainers = keepContainers;
  }

  public void setApplicationTags(Set<String> tags) {
    this.tags = tags;
  }
  
  public void setAppNodeLabelExpression(String appNodeLabelExpression) {
    this.appNodeLabelExpression = appNodeLabelExpression;
  }

  public void setAMContainerNodeLabelExpression(String nodeLabelExpression) {
    this.amContainerNodeLabelExpression = nodeLabelExpression;
  }

  public void setLogAggregationContextInfo(
      LogAggregationContextInfo logAggregationContextInfo) {
    this.logAggregationContextInfo = logAggregationContextInfo;
  }

  public void setAttemptFailuresValidityInterval(
      long attemptFailuresValidityInterval) {
    this.attemptFailuresValidityInterval = attemptFailuresValidityInterval;
  }

  public void setReservationId(String reservationId) {
    this.reservationId = reservationId;
  }
}
