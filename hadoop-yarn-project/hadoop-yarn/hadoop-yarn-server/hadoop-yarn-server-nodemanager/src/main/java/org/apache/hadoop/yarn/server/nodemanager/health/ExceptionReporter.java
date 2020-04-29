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
 * Simple {@link HealthReporter} implementation which reports whether a fatal
 * exception has happened in the NodeManager.
 *
 * See the <code>reportException</code> call of
 * {@link org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl}
 */
public class ExceptionReporter implements HealthReporter {
  private Exception nodeHealthException;
  private long nodeHealthExceptionReportTime;

  ExceptionReporter() {
    this.nodeHealthException = null;
    this.nodeHealthExceptionReportTime = 0;
  }

  @Override
  public synchronized boolean isHealthy() {
    return nodeHealthException == null;
  }

  @Override
  public synchronized String getHealthReport() {
    return nodeHealthException == null ? null :
        nodeHealthException.getMessage();
  }

  @Override
  public synchronized long getLastHealthReportTime() {
    return nodeHealthExceptionReportTime;
  }

  /**
   * Report an exception to mark the node as unhealthy.
   * @param ex the exception that makes the node unhealthy
   */
  public synchronized void reportException(Exception ex) {
    nodeHealthException = ex;
    nodeHealthExceptionReportTime = System.currentTimeMillis();
  }
}
