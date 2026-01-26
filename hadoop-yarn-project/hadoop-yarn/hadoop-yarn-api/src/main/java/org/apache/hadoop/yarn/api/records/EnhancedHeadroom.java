/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.util.Records;

/**
 * Enhanced head room in AllocateResponse.
 * This provides a channel for RMs to return load information for AMRMProxy
 * decision making when rerouting resource requests.
 *
 * Contains total pending container count and active cores for a cluster.
 */
public abstract class EnhancedHeadroom {
  public static EnhancedHeadroom newInstance(int totalPendingCount,
      int totalActiveCores) {
    EnhancedHeadroom enhancedHeadroom =
        Records.newRecord(EnhancedHeadroom.class);
    enhancedHeadroom.setTotalPendingCount(totalPendingCount);
    enhancedHeadroom.setTotalActiveCores(totalActiveCores);
    return enhancedHeadroom;
  }

  /**
   * Set total pending container count.
   * @param totalPendingCount the pending container count
   */
  public abstract void setTotalPendingCount(int totalPendingCount);

  /**
   * Get total pending container count.
   * @return the pending container count
   */
  public abstract int getTotalPendingCount();

  /**
   * Set total active cores for the cluster.
   * @param totalActiveCores the total active cores for the cluster
   */
  public abstract void setTotalActiveCores(int totalActiveCores);

  /**
   * Get total active cores for the cluster.
   * @return totalActiveCores the total active cores for the cluster
   */
  public abstract int getTotalActiveCores();

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<pendingCount:").append(this.getTotalPendingCount());
    sb.append(", activeCores:").append(this.getTotalActiveCores());
    sb.append(">");
    return sb.toString();
  }

  public double getNormalizedPendingCount(long multiplier) {
    int totalPendingCount = getTotalPendingCount();
    return (double) totalPendingCount * multiplier;
  }
}
