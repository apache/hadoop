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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.util.Comparator;

/**
 * Compares NNs in the same namespace and prioritizes by their status. The
 * priorities are:
 * <ul>
 * <li>ACTIVE
 * <li>STANDBY
 * <li>UNAVAILABLE
 * </ul>
 * When two NNs have the same state, the last modification date is the tie
 * breaker, newest has priority. Expired NNs are excluded.
 */
public class NamenodePriorityComparator
    implements Comparator<FederationNamenodeContext> {

  @Override
  public int compare(FederationNamenodeContext o1,
      FederationNamenodeContext o2) {
    FederationNamenodeServiceState state1 = o1.getState();
    FederationNamenodeServiceState state2 = o2.getState();

    if (state1 == state2) {
      // Both have the same state, use mode dates
      return compareModDates(o1, o2);
    } else {
      // Enum is ordered by priority
      return state1.compareTo(state2);
    }
  }

  /**
   * Compare the modification dates.
   *
   * @param o1 Context 1.
   * @param o2 Context 2.
   * @return Comparison between dates.
   */
  private int compareModDates(FederationNamenodeContext o1,
      FederationNamenodeContext o2) {
    // Reverse sort, lowest position is highest priority.
    return (int) (o2.getDateModified() - o1.getDateModified());
  }
}
