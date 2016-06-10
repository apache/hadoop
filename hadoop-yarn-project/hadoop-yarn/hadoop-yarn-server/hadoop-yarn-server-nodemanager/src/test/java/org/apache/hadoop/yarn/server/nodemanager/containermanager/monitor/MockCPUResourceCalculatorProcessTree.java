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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * Mock class to obtain resource usage (CPU).
 */
public class MockCPUResourceCalculatorProcessTree
    extends ResourceCalculatorProcessTree {

  private long cpuPercentage = ResourceCalculatorProcessTree.UNAVAILABLE;

  /**
   * Constructor for MockCPUResourceCalculatorProcessTree with specified root
   * process.
   * @param root
   */
  public MockCPUResourceCalculatorProcessTree(String root) {
    super(root);
  }

  @Override
  public void updateProcessTree() {
  }

  @Override
  public String getProcessTreeDump() {
    return "";
  }

  @Override
  public long getCumulativeCpuTime() {
    return 0;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return true;
  }

  @Override
  public float getCpuUsagePercent() {
    long cpu = this.cpuPercentage;
    // First getter call will be returned with -1, and other calls will
    // return non-zero value as defined below.
    if (cpu == ResourceCalculatorProcessTree.UNAVAILABLE) {
      // Set a default value other than 0 for test.
      this.cpuPercentage = 50;
    }
    return cpu;
  }
}
