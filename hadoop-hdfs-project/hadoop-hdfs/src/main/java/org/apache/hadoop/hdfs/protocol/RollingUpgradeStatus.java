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
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Rolling upgrade status
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RollingUpgradeStatus {
  private final String blockPoolId;

  public RollingUpgradeStatus(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  @Override
  public int hashCode() {
    return blockPoolId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof RollingUpgradeStatus)) {
      return false;
    }
    final RollingUpgradeStatus that = (RollingUpgradeStatus)obj;
    return this.blockPoolId.equals(that.blockPoolId);
  }

  @Override
  public String toString() {
    return "  Block Pool ID: " + blockPoolId;
  }
}
