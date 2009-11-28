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
package org.apache.hadoop.mapred;

import java.util.List;

/**
 * This class only exists to pass the current simulation time to the 
 * JobTracker in the heartbeat() call.
 */
class SimulatorTaskTrackerStatus extends TaskTrackerStatus {
  /**
   * The virtual, simulation time, when the hearbeat() call transmitting 
   * this TaskTrackerSatus occured. 
   */
  private final long currentSimulationTime;

  /**
   * Constructs a SimulatorTaskTrackerStatus object. All parameters are 
   * the same as in {@link TaskTrackerStatus}. The only extra is
   * @param currentSimulationTime the current time in the simulation when the 
   *                              heartbeat() call transmitting this 
   *                              TaskTrackerStatus occured.
   */ 
  public SimulatorTaskTrackerStatus(String trackerName, String host, 
                                    int httpPort, List<TaskStatus> taskReports, 
                                    int failures, int maxMapTasks,
                                    int maxReduceTasks,
                                    long currentSimulationTime) {
    super(trackerName, host, httpPort, taskReports,
          failures, maxMapTasks, maxReduceTasks);
    this.currentSimulationTime = currentSimulationTime;
  }

  /** 
   * Returns the current time in the simulation.
   */
  public long getCurrentSimulationTime() {
    return currentSimulationTime;
  }
}
