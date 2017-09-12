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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.StringUtils;

@Private
@Evolving
public class ResourceWeights {
  public static final ResourceWeights NEUTRAL = new ResourceWeights(1.0f);

  private final float[] weights = new float[ResourceType.values().length];

  public ResourceWeights(float memoryWeight, float cpuWeight) {
    weights[ResourceType.MEMORY.ordinal()] = memoryWeight;
    weights[ResourceType.CPU.ordinal()] = cpuWeight;
  }

  public ResourceWeights(float weight) {
    setWeight(weight);
  }

  public ResourceWeights() { }

  public final void setWeight(float weight) {
    for (int i = 0; i < weights.length; i++) {
      weights[i] = weight;
    }
  }

  public void setWeight(ResourceType resourceType, float weight) {
    weights[resourceType.ordinal()] = weight;
  }
  
  public float getWeight(ResourceType resourceType) {
    return weights[resourceType.ordinal()];
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<");
    for (int i = 0; i < ResourceType.values().length; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      ResourceType resourceType = ResourceType.values()[i];
      sb.append(StringUtils.toLowerCase(resourceType.name()));
      sb.append(StringUtils.format(" weight=%.1f", getWeight(resourceType)));
    }
    sb.append(">");
    return sb.toString();
  }
}
