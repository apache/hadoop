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
}
