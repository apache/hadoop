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
 * <p><code>LogAggregationContext</code> represents all of the
 * information needed by the <code>NodeManager</code> to handle
 * the logs for an application.</p>
 *
 * <p>It includes details such as:
 *   <ul>
 *     <li>includePattern. It uses Java Regex to filter the log files
 *     which match the defined include pattern and those log files
 *     will be uploaded. </li>
 *     <li>excludePattern. It uses Java Regex to filter the log files
 *     which match the defined exclude pattern and those log files
 *     will not be uploaded. If the log file name matches both the
 *     include and the exclude pattern, this file will be excluded eventually</li>
 *     <li>rollingIntervalSeconds. The default value is -1. By default,
 *     the logAggregationService only uploads container logs when
 *     the application is finished. This configure defines
 *     how often the logAggregationSerivce uploads container logs in seconds.
 *     By setting this configure, the logAggregationSerivce can upload container
 *     logs periodically when the application is running.
 *     </li>
 *   </ul>
 * </p>
 *
 * @see ApplicationSubmissionContext
 */

@Evolving
@Public
public abstract class LogAggregationContext {

  @Public
  @Unstable
  public static LogAggregationContext newInstance(String includePattern,
      String excludePattern, long rollingIntervalSeconds) {
    LogAggregationContext context = Records.newRecord(LogAggregationContext.class);
    context.setIncludePattern(includePattern);
    context.setExcludePattern(excludePattern);
    context.setRollingIntervalSeconds(rollingIntervalSeconds);
    return context;
  }

  /**
   * Get include pattern
   *
   * @return include pattern
   */
  @Public
  @Unstable
  public abstract String getIncludePattern();

  /**
   * Set include pattern
   *
   * @param includePattern
   */
  @Public
  @Unstable
  public abstract void setIncludePattern(String includePattern);

  /**
   * Get exclude pattern
   *
   * @return exclude pattern
   */
  @Public
  @Unstable
  public abstract String getExcludePattern();

  /**
   * Set exclude pattern
   *
   * @param excludePattern
   */
  @Public
  @Unstable
  public abstract void setExcludePattern(String excludePattern);

  /**
   * Get rollingIntervalSeconds
   *
   * @return the rollingIntervalSeconds
   */
  @Public
  @Unstable
  public abstract long getRollingIntervalSeconds();

  /**
   * Set rollingIntervalSeconds
   *
   * @param rollingIntervalSeconds
   */
  @Public
  @Unstable
  public abstract void setRollingIntervalSeconds(long rollingIntervalSeconds);
}
