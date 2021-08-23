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

package org.apache.hadoop.yarn.sls.utils;

import java.util.LinkedHashSet;
import java.util.Set;

public final class NodeUsageRanges {
  private NodeUsageRanges() {}

  /**
   * Class to store the keyword, lower-limit and upper-limit of a resource
   * within the specified lower-limit (inclusive) and upper-limit (inclusive).
   */
  public static class Range {
    private String keyword;
    private float lowerLimit, upperLimit;
    public Range(String keyword, float lowerLimit, float upperLimit) {
      this.keyword = keyword;
      this.lowerLimit = lowerLimit;
      this.upperLimit = upperLimit;
    }

    public String getKeyword() {
      return keyword;
    }

    public float getLowerLimit() {
      return lowerLimit;
    }

    public float getUpperLimit() {
      return upperLimit;
    }
  }

  private static final Set<Range> RANGES;
  static {
    RANGES = new LinkedHashSet<>();
    RANGES.add(new Range("unused", 0, 0));
    RANGES.add(new Range("1to19pctUsed", 1, 19));
    RANGES.add(new Range("20to39pctUsed", 20, 39));
    RANGES.add(new Range("40to59pctUsed", 40, 59));
    RANGES.add(new Range("60to79pctUsed", 60, 79));
    RANGES.add(new Range("80to99pctUsed", 80, 99));
    RANGES.add(new Range("full", 100, 100));
  }

  public static Set<Range> getRanges() {
    return RANGES;
  }
}
