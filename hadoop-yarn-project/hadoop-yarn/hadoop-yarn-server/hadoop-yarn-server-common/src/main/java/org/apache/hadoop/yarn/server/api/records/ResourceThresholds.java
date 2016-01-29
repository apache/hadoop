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
package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.api.records.impl.pb.ResourceThresholdsPBImpl;

/**
 * Captures resource thresholds to be used for allocation and preemption
 * when over-allocation is turned on.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ResourceThresholds {
  public static ResourceThresholds newInstance(float threshold) {
    ResourceThresholds thresholds = new ResourceThresholdsPBImpl();
    thresholds.setMemoryThreshold(threshold);
    thresholds.setCpuThreshold(threshold);
    return thresholds;
  }

  public abstract float getMemoryThreshold();

  public abstract float getCpuThreshold();

  public abstract void setMemoryThreshold(float memoryThreshold);

  public abstract void setCpuThreshold(float cpuThreshold);
}
