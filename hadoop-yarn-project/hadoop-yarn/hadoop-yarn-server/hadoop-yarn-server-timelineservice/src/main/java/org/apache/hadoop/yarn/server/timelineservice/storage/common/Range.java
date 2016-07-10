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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Encapsulates a range with start and end indices.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Range {
  private final int startIdx;
  private final int endIdx;

  /**
   * Defines a range from start index (inclusive) to end index (exclusive).
   *
   * @param start
   *          Starting index position
   * @param end
   *          Ending index position (exclusive)
   */
  public Range(int start, int end) {
    if (start < 0 || end < start) {
      throw new IllegalArgumentException(
          "Invalid range, required that: 0 <= start <= end; start=" + start
              + ", end=" + end);
    }

    this.startIdx = start;
    this.endIdx = end;
  }

  public int start() {
    return startIdx;
  }

  public int end() {
    return endIdx;
  }

  public int length() {
    return endIdx - startIdx;
  }
}