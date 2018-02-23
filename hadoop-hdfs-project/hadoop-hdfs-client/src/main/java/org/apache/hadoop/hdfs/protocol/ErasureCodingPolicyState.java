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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Value denotes the possible states of an ErasureCodingPolicy.
 */
@InterfaceAudience.Private
public enum ErasureCodingPolicyState {

  /** Policy is disabled. It's policy default state. */
  DISABLED(1),
  /** Policy is enabled. It can be applied to directory and file. */
  ENABLED(2),
  /**
   * Policy is removed from the system. Due to there are potential files
   * use this policy, it cannot be deleted from system immediately. A removed
   * policy can be re-enabled later.*/
  REMOVED(3);

  private static final ErasureCodingPolicyState[] CACHED_VALUES =
      ErasureCodingPolicyState.values();

  private final int value;

  ErasureCodingPolicyState(int v) {
    value = v;
  }

  public int getValue() {
    return value;
  }

  public static ErasureCodingPolicyState fromValue(int v) {
    if (v > 0 && v <= CACHED_VALUES.length) {
      return CACHED_VALUES[v - 1];
    }
    return null;
  }

  /** Read from in. */
  public static ErasureCodingPolicyState read(DataInput in) throws IOException {
    return fromValue(in.readByte());
  }

  /** Write to out. */
  public void write(DataOutput out) throws IOException {
    out.writeByte(ordinal());
  }
}
