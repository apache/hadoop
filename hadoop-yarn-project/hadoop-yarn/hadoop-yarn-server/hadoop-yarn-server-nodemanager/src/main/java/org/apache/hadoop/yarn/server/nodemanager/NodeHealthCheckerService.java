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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 */
public class NodeHealthCheckerService extends CompositeService {

  private NodeHealthScriptRunner nodeHealthScriptRunner;
  private LocalDirsHandlerService dirsHandler;

  static final String SEPARATOR = ";";

  public NodeHealthCheckerService() {
    super(NodeHealthCheckerService.class.getName());
    dirsHandler = new LocalDirsHandlerService();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (NodeHealthScriptRunner.shouldRun(conf)) {
      nodeHealthScriptRunner = new NodeHealthScriptRunner();
      addService(nodeHealthScriptRunner);
    }
    addService(dirsHandler);
    super.serviceInit(conf);
  }

  /**
   * @return the reporting string of health of the node
   */
  String getHealthReport() {
    String scriptReport = (nodeHealthScriptRunner == null) ? ""
        : nodeHealthScriptRunner.getHealthReport();
    if (scriptReport.equals("")) {
      return dirsHandler.getDisksHealthReport(false);
    } else {
      return scriptReport.concat(SEPARATOR + dirsHandler.getDisksHealthReport(false));
    }
  }

  /**
   * @return <em>true</em> if the node is healthy
   */
  boolean isHealthy() {
    boolean scriptHealthStatus = (nodeHealthScriptRunner == null) ? true
        : nodeHealthScriptRunner.isHealthy();
    return scriptHealthStatus && dirsHandler.areDisksHealthy();
  }

  /**
   * @return when the last time the node health status is reported
   */
  long getLastHealthReportTime() {
    long diskCheckTime = dirsHandler.getLastDisksCheckTime();
    long lastReportTime = (nodeHealthScriptRunner == null)
        ? diskCheckTime
        : Math.max(nodeHealthScriptRunner.getLastReportedTime(), diskCheckTime);
    return lastReportTime;
  }

  /**
   * @return the disk handler
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * @return the node health script runner
   */
  NodeHealthScriptRunner getNodeHealthScriptRunner() {
    return nodeHealthScriptRunner;
  }
}
