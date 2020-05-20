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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public final class CapacitySchedulerConfigGeneratorForTest {

  private CapacitySchedulerConfigGeneratorForTest() {
    throw new IllegalStateException("Utility class");
  }

  public static Configuration createConfiguration(Map<String, String> configs) {
    Configuration config = new Configuration();
    for (Map.Entry entry: configs.entrySet()) {
      config.set((String)entry.getKey(), (String)entry.getValue());
    }
    return config;
  }

  public static Configuration createBasicCSConfiguration() {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.root.queues", "test1, test2");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.test2.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.test1.maximum-capacity", "100");
    conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
    conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");
    conf.put("yarn.scheduler.capacity.queue-mappings",
            "u:test1:test1,u:test2:test2");
    return createConfiguration(conf);
  }

}
