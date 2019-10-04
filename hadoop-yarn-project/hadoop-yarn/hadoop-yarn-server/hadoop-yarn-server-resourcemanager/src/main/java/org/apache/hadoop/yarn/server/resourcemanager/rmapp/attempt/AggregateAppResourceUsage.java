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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;

import java.util.HashMap;
import java.util.Map;

@Private
public class AggregateAppResourceUsage {
  private Map<String, Long> resourceSecondsMap = new HashMap<>();

  public AggregateAppResourceUsage(Map<String, Long> resourceSecondsMap) {
    this.resourceSecondsMap.putAll(resourceSecondsMap);
  }

  /**
   * @return the memorySeconds
   */
  public long getMemorySeconds() {
    return RMServerUtils.getOrDefault(resourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  /**
   * @return the vcoreSeconds
   */
  public long getVcoreSeconds() {
    return RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L);
  }

  public Map<String, Long> getResourceUsageSecondsMap() {
    return resourceSecondsMap;
  }
}
