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
package org.apache.hadoop.hdfs.protocol;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

/**
 * Metadata about a snapshottable directory.
 */
public class SnapshotStatus {
  /**
   * Basic information of the snapshot directory.
   */
  private final HdfsFileStatus dirStatus;

  /**
   * Snapshot ID for the snapshot.
   */
  private final int snapshotID;

  /**
   * Whether the snapshot is deleted or not.
   */
  private final boolean isDeleted;

  /**
   * Full path of the parent.
   */
  private byte[] parentFullPath;

  public SnapshotStatus(long modificationTime, long accessTime,
                        FsPermission permission,
                        EnumSet<HdfsFileStatus.Flags> flags,
                        String owner, String group, byte[] localName,
                        long inodeId, int childrenNum, int snapshotID,
                        boolean isDeleted, byte[] parentFullPath) {
    this.dirStatus = new HdfsFileStatus.Builder()
        .isdir(true)
        .mtime(modificationTime)
        .atime(accessTime)
        .perm(permission)
        .flags(flags)
        .owner(owner)
        .group(group)
        .path(localName)
        .fileId(inodeId)
        .children(childrenNum)
        .build();
    this.snapshotID = snapshotID;
    this.isDeleted = isDeleted;
    this.parentFullPath = parentFullPath;
  }

  public SnapshotStatus(HdfsFileStatus dirStatus,
                                      int snapshotID, boolean isDeleted,
                                      byte[] parentFullPath) {
    this.dirStatus = dirStatus;
    this.snapshotID = snapshotID;
    this.isDeleted = isDeleted;
    this.parentFullPath = parentFullPath;
  }

  /**
   * sets the path name.
   * @param path path
   */
  public void setParentFullPath(byte[] path) {
    parentFullPath = path;
  }

  /**
   * @return snapshot id for the snapshot
   */
  public int getSnapshotID() {
    return snapshotID;
  }

  /**
   * @return whether snapshot is deleted
   */
  public boolean isDeleted() {
    return isDeleted;
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
  public byte[] getParentFullPath() {
    return parentFullPath;
  }

  /**
   * @return Full path of the snapshot
   */
  public Path getFullPath() {
    String parentFullPathStr =
        (parentFullPath == null || parentFullPath.length == 0) ?
            "/" : DFSUtilClient.bytes2String(parentFullPath);
    return new Path(getSnapshotPath(parentFullPathStr,
        dirStatus.getLocalName()));
  }

  /**
   * Print a list of {@link SnapshotStatus} out to a given stream.
   *
   * @param stats The list of {@link SnapshotStatus}
   * @param out   The given stream for printing.
   */
  public static void print(SnapshotStatus[] stats,
                           PrintStream out) {
    if (stats == null || stats.length == 0) {
      out.println();
      return;
    }
    int maxRepl = 0, maxLen = 0, maxOwner = 0, maxGroup = 0;
    int maxSnapshotID = 0;
    for (SnapshotStatus status : stats) {
      maxRepl = maxLength(maxRepl, status.dirStatus.getReplication());
      maxLen = maxLength(maxLen, status.dirStatus.getLen());
      maxOwner = maxLength(maxOwner, status.dirStatus.getOwner());
      maxGroup = maxLength(maxGroup, status.dirStatus.getGroup());
      maxSnapshotID = maxLength(maxSnapshotID, status.snapshotID);
    }

    String lineFormat = "%s%s " // permission string
        + "%" + maxRepl + "s "
        + (maxOwner > 0 ? "%-" + maxOwner + "s " : "%s")
        + (maxGroup > 0 ? "%-" + maxGroup + "s " : "%s")
        + "%" + maxLen + "s "
        + "%s " // mod time
        + "%" + maxSnapshotID + "s "
        + "%s " // deletion status
        + "%s"; // path
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    for (SnapshotStatus status : stats) {
      String line = String.format(lineFormat, "d",
          status.dirStatus.getPermission(),
          status.dirStatus.getReplication(),
          status.dirStatus.getOwner(),
          status.dirStatus.getGroup(),
          String.valueOf(status.dirStatus.getLen()),
          dateFormat.format(new Date(status.dirStatus.getModificationTime())),
          status.snapshotID,
          status.isDeleted ? "DELETED" : "ACTIVE",
          getSnapshotPath(DFSUtilClient.bytes2String(status.parentFullPath),
              status.dirStatus.getLocalName())
      );
      out.println(line);
    }
  }

  private static int maxLength(int n, Object value) {
    return Math.max(n, String.valueOf(value).length());
  }

  public static String getSnapshotPath(String snapshottableDir,
                                String snapshotRelativePath) {
    String parentFullPathStr =
        snapshottableDir == null || snapshottableDir.isEmpty() ?
            "/" : snapshottableDir;
    final StringBuilder b = new StringBuilder(parentFullPathStr);
    if (b.charAt(b.length() - 1) != Path.SEPARATOR_CHAR) {
      b.append(Path.SEPARATOR);
    }
    return b.append(HdfsConstants.DOT_SNAPSHOT_DIR)
        .append(Path.SEPARATOR)
        .append(snapshotRelativePath)
        .toString();
  }

  public static String getParentPath(String snapshotPath) {
    int index = snapshotPath.indexOf(HdfsConstants.DOT_SNAPSHOT_DIR);
    return index == -1 ? snapshotPath : snapshotPath.substring(0, index - 1);
  }
}