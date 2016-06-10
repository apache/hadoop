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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

import java.util.EnumSet;

public class ResourceCalculatorUtils {
  public static int divideAndCeil(long a, long b) {
    if (b == 0) {
      return 0;
    }
    return (int) ((a + (b - 1)) / b);
  }

  public static int computeAvailableContainers(Resource available,
      Resource required, EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.min(
        calculateRatioOrMaxValue(available.getMemorySize(), required.getMemorySize()),
        calculateRatioOrMaxValue(available.getVirtualCores(), required
            .getVirtualCores()));
    }
    return calculateRatioOrMaxValue(
      available.getMemorySize(), required.getMemorySize());
  }

  public static int divideAndCeilContainers(Resource required, Resource factor,
      EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.max(divideAndCeil(required.getMemorySize(), factor.getMemorySize()),
        divideAndCeil(required.getVirtualCores(), factor.getVirtualCores()));
    }
    return divideAndCeil(required.getMemorySize(), factor.getMemorySize());
  }

  private static int calculateRatioOrMaxValue(long numerator, long denominator) {
    if (denominator == 0) {
      return Integer.MAX_VALUE;
    }
    return (int) (numerator / denominator);
  }
}
