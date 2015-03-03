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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResourceUsage {
  private static final Log LOG = LogFactory.getLog(TestResourceUsage.class);
  private String suffix;

  @Parameterized.Parameters
  public static Collection<String[]> getParameters() {
    return Arrays.asList(new String[][] { { "Pending" }, { "Used" },
        { "Reserved" }, { "AMUsed" } });
  }

  public TestResourceUsage(String suffix) {
    this.suffix = suffix;
  }

  private static void dec(ResourceUsage obj, String suffix, Resource res,
      String label) throws Exception {
    executeByName(obj, "dec" + suffix, res, label);
  }

  private static void inc(ResourceUsage obj, String suffix, Resource res,
      String label) throws Exception {
    executeByName(obj, "inc" + suffix, res, label);
  }

  private static void set(ResourceUsage obj, String suffix, Resource res,
      String label) throws Exception {
    executeByName(obj, "set" + suffix, res, label);
  }

  private static Resource get(ResourceUsage obj, String suffix, String label)
      throws Exception {
    return executeByName(obj, "get" + suffix, null, label);
  }

  // Use reflection to avoid too much avoid code
  private static Resource executeByName(ResourceUsage obj, String methodName,
      Resource arg, String label) throws Exception {
    // We have 4 kinds of method
    // 1. getXXX() : Resource
    // 2. getXXX(label) : Resource
    // 3. set/inc/decXXX(res) : void
    // 4. set/inc/decXXX(label, res) : void
    if (methodName.startsWith("get")) {
      Resource result;
      if (label == null) {
        // 1.
        Method method = ResourceUsage.class.getDeclaredMethod(methodName);
        result = (Resource) method.invoke(obj);
      } else {
        // 2.
        Method method =
            ResourceUsage.class.getDeclaredMethod(methodName, String.class);
        result = (Resource) method.invoke(obj, label);
      }
      return result;
    } else {
      if (label == null) {
        // 3.
        Method method =
            ResourceUsage.class.getDeclaredMethod(methodName, Resource.class);
        method.invoke(obj, arg);
      } else {
        // 4.
        Method method =
            ResourceUsage.class.getDeclaredMethod(methodName, String.class,
                Resource.class);
        method.invoke(obj, label, arg);
      }
      return null;
    }
  }

  private void internalTestModifyAndRead(String label) throws Exception {
    ResourceUsage usage = new ResourceUsage();
    Resource res;

    // First get returns 0 always
    res = get(usage, suffix, label);
    check(0, 0, res);

    // Add 1,1 should returns 1,1
    inc(usage, suffix, Resource.newInstance(1, 1), label);
    check(1, 1, get(usage, suffix, label));

    // Set 2,2
    set(usage, suffix, Resource.newInstance(2, 2), label);
    check(2, 2, get(usage, suffix, label));

    // dec 2,2
    dec(usage, suffix, Resource.newInstance(2, 2), label);
    check(0, 0, get(usage, suffix, label));
  }

  void check(int mem, int cpu, Resource res) {
    Assert.assertEquals(mem, res.getMemory());
    Assert.assertEquals(cpu, res.getVirtualCores());
  }

  @Test
  public void testModifyAndRead() throws Exception {
    LOG.info("Test - " + suffix);
    internalTestModifyAndRead(null);
    internalTestModifyAndRead("label");
  }
}
