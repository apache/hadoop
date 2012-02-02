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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/*
 * This object gathers the [currently four] PeriodStatset's that we
 * are gathering for a particular task attempt for packaging and
 * handling as a single object.
 */
@Private
@Unstable
public class ProgressSplitsBlock {
  final PeriodicStatsAccumulator progressWallclockTime;
  final PeriodicStatsAccumulator progressCPUTime;
  final PeriodicStatsAccumulator progressVirtualMemoryKbytes;
  final PeriodicStatsAccumulator progressPhysicalMemoryKbytes;

  static final int[] NULL_ARRAY = new int[0];

  static final int WALLCLOCK_TIME_INDEX = 0;
  static final int CPU_TIME_INDEX = 1;
  static final int VIRTUAL_MEMORY_KBYTES_INDEX = 2;
  static final int PHYSICAL_MEMORY_KBYTES_INDEX = 3;

  static final int DEFAULT_NUMBER_PROGRESS_SPLITS = 12;

  ProgressSplitsBlock(int numberSplits) {
    progressWallclockTime
      = new CumulativePeriodicStats(numberSplits);
    progressCPUTime
      = new CumulativePeriodicStats(numberSplits);
    progressVirtualMemoryKbytes
      = new StatePeriodicStats(numberSplits);
    progressPhysicalMemoryKbytes
      = new StatePeriodicStats(numberSplits);
  }

  // this coordinates with LoggedTaskAttempt.SplitVectorKind
  int[][] burst() {
    int[][] result = new int[4][];

    result[WALLCLOCK_TIME_INDEX] = progressWallclockTime.getValues();
    result[CPU_TIME_INDEX] = progressCPUTime.getValues();
    result[VIRTUAL_MEMORY_KBYTES_INDEX] = progressVirtualMemoryKbytes.getValues();
    result[PHYSICAL_MEMORY_KBYTES_INDEX] = progressPhysicalMemoryKbytes.getValues();

    return result;
  }

  static public int[] arrayGet(int[][] burstedBlock, int index) {
    return burstedBlock == null ? NULL_ARRAY : burstedBlock[index];
  }

  static public int[] arrayGetWallclockTime(int[][] burstedBlock) {
    return arrayGet(burstedBlock, WALLCLOCK_TIME_INDEX);
  }

  static public int[] arrayGetCPUTime(int[][] burstedBlock) {
    return arrayGet(burstedBlock, CPU_TIME_INDEX);
  }

  static public int[] arrayGetVMemKbytes(int[][] burstedBlock) {
    return arrayGet(burstedBlock, VIRTUAL_MEMORY_KBYTES_INDEX);
  }

  static public int[] arrayGetPhysMemKbytes(int[][] burstedBlock) {
    return arrayGet(burstedBlock, PHYSICAL_MEMORY_KBYTES_INDEX);
  }
}
    
