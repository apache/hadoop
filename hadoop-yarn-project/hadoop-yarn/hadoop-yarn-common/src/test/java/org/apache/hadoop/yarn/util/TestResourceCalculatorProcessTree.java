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
package org.apache.hadoop.yarn.util;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A JUnit test to test {@link ResourceCalculatorPlugin}
 */
public class TestResourceCalculatorProcessTree {

  public static class EmptyProcessTree extends ResourceCalculatorProcessTree {

    public EmptyProcessTree(String pid) {
      super(pid);
    }

    public void updateProcessTree() {
    }

    public String getProcessTreeDump() {
      return "Empty tree for testing";
    }

    public long getRssMemorySize(int age) {
      return 0;
    }
    @SuppressWarnings("deprecation")
    public long getCumulativeRssmem(int age) {
      return 0;
    }

    public long getVirtualMemorySize(int age) {
      return 0;
    }

    @SuppressWarnings("deprecation")
    public long getCumulativeVmem(int age) {
      return 0;
    }

    public long getCumulativeCpuTime() {
      return 0;
    }

    @Override
    public float getCpuUsagePercent() {
      return UNAVAILABLE;
    }

    public boolean checkPidPgrpidForMatch() {
      return false;
    }
  }

  @Test
  void testCreateInstance() {
    ResourceCalculatorProcessTree tree;
    tree = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree("1", EmptyProcessTree.class, new Configuration());
    assertNotNull(tree);
    assertThat(tree, instanceOf(EmptyProcessTree.class));
  }

  @Test
  void testCreatedInstanceConfigured() {
    ResourceCalculatorProcessTree tree;
    Configuration conf = new Configuration();
    tree = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree("1", EmptyProcessTree.class, conf);
    assertNotNull(tree);
    assertThat(tree.getConf(), sameInstance(conf));
  } 
}
