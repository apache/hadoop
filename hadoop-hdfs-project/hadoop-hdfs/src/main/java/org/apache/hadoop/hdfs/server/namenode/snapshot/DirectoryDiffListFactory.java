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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.slf4j.Logger;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;

/** For creating {@link DiffList} for {@link DirectoryDiff}. */
public abstract class DirectoryDiffListFactory {
  public static DiffList<DirectoryDiff> createDiffList(int capacity) {
    return constructor.apply(capacity);
  }

  public static void init(int interval, int maxSkipLevels, Logger log) {
    DirectoryDiffListFactory.skipInterval = interval;
    DirectoryDiffListFactory.maxLevels = maxSkipLevels;

    if (maxLevels > 0) {
      constructor = c -> new DiffListBySkipList(c);
      log.info("SkipList is enabled with skipInterval=" + skipInterval
          + ", maxLevels=" + maxLevels);
    } else {
      constructor = c -> new DiffListByArrayList<>(c);
      log.info("SkipList is disabled");
    }
  }

  private static volatile IntFunction<DiffList<DirectoryDiff>> constructor
      = c -> new DiffListByArrayList<>(c);

  private static volatile int skipInterval;
  private static volatile int maxLevels;

  /**
   * Returns the level of a skip list node.
   * @return A value in the range 0 to maxLevels.
   */
  public static int randomLevel() {
    final Random r = ThreadLocalRandom.current();
    for (int level = 0; level < maxLevels; level++) {
      // skip to the next level with probability 1/skipInterval
      if (r.nextInt(skipInterval) > 0) {
        return level;
      }
    }
    return maxLevels;
  }


  private DirectoryDiffListFactory() {}
}
