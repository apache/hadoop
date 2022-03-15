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
 * Mock class to obtain resource usage (Memory).
 */
public class MockMemoryResourceCalculatorProcessTree extends ResourceCalculatorProcessTree {
  private final long memorySize = 500000000L;

  private long rssMemorySize = memorySize;
  private long virtualMemorySize = ResourceCalculatorProcessTree.UNAVAILABLE;

  /**
   * Constructor for MockMemoryResourceCalculatorProcessTree with specified root
   * process.
   * @param root
   */
  public MockMemoryResourceCalculatorProcessTree(String root) {
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
  public long getRssMemorySize(int olderThanAge) {
    long rssMemory = this.rssMemorySize;
    // First getter call will return with 500000000, and second call will
    // return -1, rest of the calls will return a valid value.
    if (rssMemory == memorySize) {
      this.rssMemorySize = ResourceCalculatorProcessTree.UNAVAILABLE;
    }
    if (rssMemory == ResourceCalculatorProcessTree.UNAVAILABLE) {
      this.rssMemorySize = 2 * memorySize;
    }
    return rssMemory;
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    long virtualMemory = this.virtualMemorySize;
    // First getter call will return with -1, and rest of the calls will
    // return a valid value.
    if (virtualMemory == ResourceCalculatorProcessTree.UNAVAILABLE) {
      this.virtualMemorySize = 3 * memorySize;
    }
    return virtualMemory;
  }

  @Override
  public float getCpuUsagePercent() {
    return 0;
  }
}
