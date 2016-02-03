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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Simple class to allow users to send information required to create a
 * ContainerLaunchContext which can then be used as part of the
 * ApplicationSubmissionContext
 *
 */
@XmlRootElement(name = "log-aggregation-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogAggregationContextInfo {

  @XmlElement(name = "log-include-pattern")
  String logIncludePattern;

  @XmlElement(name = "log-exclude-pattern")
  String logExcludePattern;

  @XmlElement(name = "rolled-log-include-pattern")
  String rolledLogsIncludePattern;

  @XmlElement(name = "rolled-log-exclude-pattern")
  String rolledLogsExcludePattern;

  @XmlElement(name = "log-aggregation-policy-class-name")
  String policyClassName;

  @XmlElement(name = "log-aggregation-policy-parameters")
  String policyParameters;

  public LogAggregationContextInfo() {
  }

  public String getIncludePattern() {
    return this.logIncludePattern;
  }

  public void setIncludePattern(String includePattern) {
    this.logIncludePattern = includePattern;
  }

  public String getExcludePattern() {
    return this.logExcludePattern;
  }

  public void setExcludePattern(String excludePattern) {
    this.logExcludePattern = excludePattern;
  }

  public String getRolledLogsIncludePattern() {
    return this.rolledLogsIncludePattern;
  }

  public void setRolledLogsIncludePattern(
      String rolledLogsIncludePattern) {
    this.rolledLogsIncludePattern = rolledLogsIncludePattern;
  }

  public String getRolledLogsExcludePattern() {
    return this.rolledLogsExcludePattern;
  }

  public void setRolledLogsExcludePattern(
      String rolledLogsExcludePattern) {
    this.rolledLogsExcludePattern = rolledLogsExcludePattern;
  }

  public String getLogAggregationPolicyClassName() {
    return this.policyClassName;
  }

  public void setLogAggregationPolicyClassName(
      String className) {
    this.policyClassName = className;
  }

  public String getLogAggregationPolicyParameters() {
    return this.policyParameters;
  }

  public void setLogAggregationPolicyParameters(
      String parameters) {
    this.policyParameters = parameters;
  }
}
