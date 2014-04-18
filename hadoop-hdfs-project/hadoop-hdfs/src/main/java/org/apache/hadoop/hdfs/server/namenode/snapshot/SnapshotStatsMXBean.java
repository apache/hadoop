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

import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

/**
 * This is an interface used to retrieve statistic information related to
 * snapshots
 */
public interface SnapshotStatsMXBean {

  /**
   * Return the list of snapshottable directories
   *
   * @return the list of snapshottable directories
   */
  public SnapshottableDirectoryStatus.Bean[] getSnapshottableDirectories();

  /**
   * Return the list of snapshots
   *
   * @return the list of snapshots
   */
  public SnapshotInfo.Bean[] getSnapshots();

}
