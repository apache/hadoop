/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.regionserver.compactions;

/**
 * This class holds information relevant for tracking the progress of a
 * compaction.
 *
 * <p>The metrics tracked allow one to calculate the percent completion of the
 * compaction based on the number of Key/Value pairs already compacted vs.
 * total amount scheduled to be compacted.
 *
 */
public class CompactionProgress {

  /** the total compacting key values in currently running compaction */
  public long totalCompactingKVs;
  /** the completed count of key values in currently running compaction */
  public long currentCompactedKVs = 0;

  /** Constructor
   * @param totalCompactingKVs the total Key/Value pairs to be compacted
   */
  public CompactionProgress(long totalCompactingKVs) {
    this.totalCompactingKVs = totalCompactingKVs;
  }

  /** getter for calculated percent complete
   * @return float
   */
  public float getProgressPct() {
    return currentCompactedKVs / totalCompactingKVs;
  }

}
