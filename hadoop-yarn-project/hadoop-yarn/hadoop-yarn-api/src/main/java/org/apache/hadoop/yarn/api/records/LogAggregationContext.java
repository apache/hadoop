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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code LogAggregationContext} represents all of the
 * information needed by the {@code NodeManager} to handle
 * the logs for an application.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>
 *     includePattern. It uses Java Regex to filter the log files
 *     which match the defined include pattern and those log files
 *     will be uploaded when the application finishes.
 *   </li>
 *   <li>
 *     excludePattern. It uses Java Regex to filter the log files
 *     which match the defined exclude pattern and those log files
 *     will not be uploaded when application finishes. If the log file
 *     name matches both the include and the exclude pattern, this file
 *     will be excluded eventually.
 *   </li>
 *   <li>
 *     rolledLogsIncludePattern. It uses Java Regex to filter the log files
 *     which match the defined include pattern and those log files
 *     will be aggregated in a rolling fashion.
 *   </li>
 *   <li>
 *     rolledLogsExcludePattern. It uses Java Regex to filter the log files
 *     which match the defined exclude pattern and those log files
 *     will not be aggregated in a rolling fashion. If the log file
 *     name matches both the include and the exclude pattern, this file
 *     will be excluded eventually.
 *   </li>
 *   <li>
 *     policyClassName. The policy class name that implements
 *     ContainerLogAggregationPolicy. At runtime, nodemanager will the policy
 *     if a given container's log should be aggregated based on the
 *     ContainerType and other runtime state such as exit code by calling
 *     ContainerLogAggregationPolicy#shouldDoLogAggregation.
 *     This is useful when the app only wants to aggregate logs of a subset of
 *     containers. Here are the available policies. Please make sure to specify
 *     the canonical name by prefixing org.apache.hadoop.yarn.server.
 *     nodemanager.containermanager.logaggregation.
 *     to the class simple name below.
 *     NoneContainerLogAggregationPolicy: skip aggregation for all containers.
 *     AllContainerLogAggregationPolicy: aggregate all containers.
 *     AMOrFailedContainerLogAggregationPolicy: aggregate application master
 *         or failed containers.
 *     FailedOrKilledContainerLogAggregationPolicy: aggregate failed or killed
 *         containers
 *     FailedContainerLogAggregationPolicy: aggregate failed containers
 *     AMOnlyLogAggregationPolicy: aggregate application master containers
 *     SampleContainerLogAggregationPolicy: sample logs of successful worker
 *         containers, in addition to application master and failed/killed
 *         containers.
 *     If it isn't specified, it will use the cluster-wide default policy
 *     defined by configuration yarn.nodemanager.log-aggregation.policy.class.
 *     The default value of yarn.nodemanager.log-aggregation.policy.class is
 *     AllContainerLogAggregationPolicy.
 *   </li>
 *   <li>
 *     policyParameters. The parameters passed to the policy class via
 *     ContainerLogAggregationPolicy#parseParameters during the policy object
 *     initialization. This is optional. Some policy class might use parameters
 *     to adjust its settings. It is up to policy class to define the scheme of
 *     parameters.
 *     For example, SampleContainerLogAggregationPolicy supports the format of
 *     "SR:0.5,MIN:50", which means sample rate of 50% beyond the first 50
 *     successful worker containers.
 *   </li>
 * </ul>
 *
 * @see ApplicationSubmissionContext
 */

@Evolving
@Public
public abstract class LogAggregationContext {

  @Public
  @Unstable
  public static LogAggregationContext newInstance(String includePattern,
      String excludePattern) {
    LogAggregationContext context = Records.newRecord(LogAggregationContext.class);
    context.setIncludePattern(includePattern);
    context.setExcludePattern(excludePattern);
    return context;
  }

  @Public
  @Unstable
  public static LogAggregationContext newInstance(String includePattern,
      String excludePattern, String rolledLogsIncludePattern,
      String rolledLogsExcludePattern) {
    LogAggregationContext context =
        Records.newRecord(LogAggregationContext.class);
    context.setIncludePattern(includePattern);
    context.setExcludePattern(excludePattern);
    context.setRolledLogsIncludePattern(rolledLogsIncludePattern);
    context.setRolledLogsExcludePattern(rolledLogsExcludePattern);
    return context;
  }

  @Public
  @Unstable
  public static LogAggregationContext newInstance(String includePattern,
      String excludePattern, String rolledLogsIncludePattern,
      String rolledLogsExcludePattern, String policyClassName,
      String policyParameters) {
    LogAggregationContext context =
        Records.newRecord(LogAggregationContext.class);
    context.setIncludePattern(includePattern);
    context.setExcludePattern(excludePattern);
    context.setRolledLogsIncludePattern(rolledLogsIncludePattern);
    context.setRolledLogsExcludePattern(rolledLogsExcludePattern);
    context.setLogAggregationPolicyClassName(policyClassName);
    context.setLogAggregationPolicyParameters(policyParameters);
    return context;
  }

  /**
   * Get include pattern. This includePattern only takes affect
   * on logs that exist at the time of application finish.
   *
   * @return include pattern
   */
  @Public
  @Unstable
  public abstract String getIncludePattern();

  /**
   * Set include pattern. This includePattern only takes affect
   * on logs that exist at the time of application finish.
   *
   * @param includePattern
   */
  @Public
  @Unstable
  public abstract void setIncludePattern(String includePattern);

  /**
   * Get exclude pattern. This excludePattern only takes affect
   * on logs that exist at the time of application finish.
   *
   * @return exclude pattern
   */
  @Public
  @Unstable
  public abstract String getExcludePattern();

  /**
   * Set exclude pattern. This excludePattern only takes affect
   * on logs that exist at the time of application finish.
   *
   * @param excludePattern
   */
  @Public
  @Unstable
  public abstract void setExcludePattern(String excludePattern);

  /**
   * Get include pattern in a rolling fashion.
   * 
   * @return include pattern
   */
  @Public
  @Unstable
  public abstract String getRolledLogsIncludePattern();

  /**
   * Set include pattern in a rolling fashion.
   * 
   * @param rolledLogsIncludePattern
   */
  @Public
  @Unstable
  public abstract void setRolledLogsIncludePattern(
      String rolledLogsIncludePattern);

  /**
   * Get exclude pattern for aggregation in a rolling fashion.
   * 
   * @return exclude pattern
   */
  @Public
  @Unstable
  public abstract String getRolledLogsExcludePattern();

  /**
   * Set exclude pattern for in a rolling fashion.
   * 
   * @param rolledLogsExcludePattern
   */
  @Public
  @Unstable
  public abstract void setRolledLogsExcludePattern(
      String rolledLogsExcludePattern);

  /**
   * Get the log aggregation policy class.
   *
   * @return log aggregation policy class
   */
  @Public
  @Unstable
  public abstract String getLogAggregationPolicyClassName();

  /**
   * Set the log aggregation policy class.
   *
   * @param className
   */
  @Public
  @Unstable
  public abstract void setLogAggregationPolicyClassName(
      String className);

  /**
   * Get the log aggregation policy parameters.
   *
   * @return log aggregation policy parameters
   */
  @Public
  @Unstable
  public abstract String getLogAggregationPolicyParameters();

  /**
   * Set the log aggregation policy parameters.
   * There is no schema defined for the parameters string.
   * It is up to the log aggregation policy class to decide how to parse
   * the parameters string.
   *
   * @param parameters
   */
  @Public
  @Unstable
  public abstract void setLogAggregationPolicyParameters(
      String parameters);
}
