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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;

import java.util.function.IntFunction;

/** For creating {@link DiffList} for {@link DirectoryDiff}. */
public abstract class DirectoryDiffListFactory {
  public static DiffList<DirectoryDiff> createDiffList(int capacity) {
    return constructor.apply(capacity);
  }

  public static void init(int skipInterval, int maxLevels, Log log) {
    if (maxLevels > 0) {
      constructor = c -> new DiffListBySkipList(c, skipInterval, maxLevels);
      log.info("SkipList is enabled with skipInterval=" + skipInterval
          + ", maxLevels=" + maxLevels);
    } else {
      constructor = c -> new DiffListByArrayList<>(c);
      log.info("SkipList is disabled");
    }
  }

  private static volatile IntFunction<DiffList<DirectoryDiff>> constructor
      = c -> new DiffListByArrayList<>(c);

  private DirectoryDiffListFactory() {}
}
