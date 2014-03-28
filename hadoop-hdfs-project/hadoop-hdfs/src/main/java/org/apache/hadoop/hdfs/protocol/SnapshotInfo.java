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
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsPermissionProto;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * SnapshotInfo maintains information for a snapshot
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SnapshotInfo {
  private final String snapshotName;
  private final String snapshotRoot;
  private final String createTime;
  private final FsPermissionProto permission;
  private final String owner;
  private final String group;

  public SnapshotInfo(String sname, String sroot, String ctime,
      FsPermissionProto permission, String owner, String group) {
    this.snapshotName = sname;
    this.snapshotRoot = sroot;
    this.createTime = ctime;
    this.permission = permission;
    this.owner = owner;
    this.group = group;
  }

  final public String getSnapshotName() {
    return snapshotName;
  }

  final public String getSnapshotRoot() {
    return snapshotRoot;
  }

  final public String getCreateTime() {
    return createTime;
  }
  
  final public FsPermissionProto getPermission() {
    return permission;
  }
  
  final public String getOwner() {
    return owner;
  }
  
  final public String getGroup() {
    return group;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{snapshotName=" + snapshotName
        + "; snapshotRoot=" + snapshotRoot
        + "; createTime=" + createTime
        + "; permission=" + permission
        + "; owner=" + owner
        + "; group=" + group
        + "}";
  }

  public static class Bean {
    private final String snapshotID;
    private final String snapshotDirectory;
    private final long modificationTime;

    public Bean(String snapshotID, String snapshotDirectory,
        long modificationTime) {
      this.snapshotID = snapshotID;
      this.snapshotDirectory = snapshotDirectory;
      this.modificationTime = modificationTime;
    }

    public String getSnapshotID() {
      return snapshotID;
    }

    public String getSnapshotDirectory() {
      return snapshotDirectory;
    }

    public long getModificationTime() {
      return modificationTime;
    }
  }
}
