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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;

/**
 * Metadata about a snapshottable directory
 */
public class SnapshottableDirectoryStatus {
  /** Basic information of the snapshottable directory */
  private HdfsFileStatus dirStatus;
  
  /** Number of snapshots that have been taken*/
  private int snapshotNumber;
  
  /** Number of snapshots allowed. */
  private int snapshotQuota;
  
  /** Full path of the parent. */
  private byte[] parentFullPath;
  
  public SnapshottableDirectoryStatus(long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] localName,
      int snapshotNumber, int snapshotQuota, byte[] parentFullPath) {
//TODO: fix fileId
    this.dirStatus = new HdfsFileStatus(0, true, 0, 0, modification_time,
        access_time, permission, owner, group, null, localName, 0L);
    this.snapshotNumber = snapshotNumber;
    this.snapshotQuota = snapshotQuota;
    this.parentFullPath = parentFullPath;
  }

  /**
   * @return Number of snapshots that have been taken for the directory
   */
  public int getSnapshotNumber() {
    return snapshotNumber;
  }

  /**
   * @return Number of snapshots allowed for the directory
   */
  public int getSnapshotQuota() {
    return snapshotQuota;
  }
  
  /**
   * @return Full path of the parent
   */
  public byte[] getParentFullPath() {
    return parentFullPath;
  }

  /**
   * @return The basic information of the directory
   */
  public HdfsFileStatus getDirStatus() {
    return dirStatus;
  }
  
  /**
   * @return Full path of the file
   */
  public Path getFullPath() {
    String parentFullPathStr = (parentFullPath == null || parentFullPath.length == 0) ? null
        : DFSUtil.bytes2String(parentFullPath);
    return parentFullPathStr == null ? new Path(dirStatus.getLocalName())
        : new Path(parentFullPathStr, dirStatus.getLocalName());
  }
}
