/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncMountSnapshotUpdateTrackerFactory {
  private static final Logger LOG = LoggerFactory
      .getLogger(SyncMountSnapshotUpdateTrackerFactory.class);

  /**
   * Currently, only support to create SyncMountSnapshotUpdateTrackerImpl.
   */
  public static SyncMountSnapshotUpdateTracker create(
      PhasedPlan planFromDiffReport,
      BlockAliasMap.Writer<FileRegion> aliasMapWriter, Configuration conf) {
    TrackerClass trackerClass = TrackerClass.ALL_MULTIPART;
    LOG.info("Creating SyncMountSnapshotUpdateTrackerImpl with {} strategy",
        trackerClass.name());
    switch (trackerClass) {
    case ALL_MULTIPART:
      return new SyncMountSnapshotUpdateTrackerImpl(planFromDiffReport,
          aliasMapWriter, conf);
    }
    throw new IllegalArgumentException("Wrong tracker class " +
        "specified in config");
  }

  public enum TrackerClass {
    ALL_MULTIPART
  }
}
