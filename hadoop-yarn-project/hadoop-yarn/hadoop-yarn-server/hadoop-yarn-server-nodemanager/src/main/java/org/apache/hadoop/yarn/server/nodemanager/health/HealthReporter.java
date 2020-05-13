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

package org.apache.hadoop.yarn.server.nodemanager.health;

/**
 * Interface providing information about the health of a service.
 *
 * Associated pieces of information:
 * <ul>
 * <li>whether the service is healthy ({@link #isHealthy()})</li>
 * <li>report of the healthiness ({@link #getHealthReport()})</li>
 * <li>latest timestamp of the health check
 * ({@link #getLastHealthReportTime()})</li>
 * </ul>
 *
 * Classes implementing this interface are used in
 * {@link NodeHealthCheckerService}.
 *
 * Developers are discouraged to implement new Java-based health scripts,
 * they should rather try to implement it as a script and use the
 * {@link NodeHealthScriptRunner} implementation.
 *
 * @see TimedHealthReporterService
 * @see org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService
 */
public interface HealthReporter {

  /**
   * Gets whether the node is healthy or not.
   *
   * @return true if node is healthy
   */
  boolean isHealthy();

  /**
   * Returns output from health check. If node is healthy then an empty string
   * is returned.
   *
   * @return output from health check
   */
  String getHealthReport();

  /**
   * Returns time stamp when node health check was last run.
   *
   * @return timestamp when node health script was last run
   */
  long getLastHealthReportTime();
}
