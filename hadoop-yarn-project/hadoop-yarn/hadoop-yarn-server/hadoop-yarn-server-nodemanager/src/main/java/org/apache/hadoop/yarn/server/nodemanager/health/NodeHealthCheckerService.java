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

import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;

/**
 * This is the base class for NodeHealthCheckerService.
 * The default implementation is {@link NodeHealthCheckerServiceImpl}
 */
public abstract class NodeHealthCheckerService extends CompositeService {

  public NodeHealthCheckerService(String name) {
    super(name);
  }

  /**
   * Joining the health reports of the dependent services.
   *
   * @return the report string about the health of the node
   */
  public abstract String getHealthReport();

  /**
   * @return <em>true</em> if the node is healthy
   */
  public abstract boolean isHealthy();

  /**
   * @return when the last time the node health status is reported
   */
  public abstract long getLastHealthReportTime();

  /**
   * @return the disk handler
   */
  public abstract LocalDirsHandlerService getDiskHandler();

  /**
   * Propagating an exception to {@link ExceptionReporter}.
   * @param exception the exception to propagate
   */
  public abstract void reportException(Exception exception);
}
