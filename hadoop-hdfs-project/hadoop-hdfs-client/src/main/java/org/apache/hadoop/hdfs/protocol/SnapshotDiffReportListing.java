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

import java.util.Collections;
import java.util.List;

import org.apache.curator.shaded.com.google.common.base.Preconditions;

import org.apache.hadoop.hdfs.DFSUtilClient;

/**
 * This class represents to  the difference between two snapshots of
 * the same directory, or the difference between a snapshot of the directory and
 * its current state. This Class serves the purpose of collecting diff entries
 * in 3 lists : created, deleted and modified list combined size of which is set
 * by dfs.snapshotdiff-report.limit over one rpc call to the namenode.
 */
public class SnapshotDiffReportListing {
  /**
   * Representing the full path and diff type of a file/directory where changes
   * have happened.
   */
  public static class DiffReportListingEntry {
    /**
     * The type of the difference.
     */
    private final long fileId;
    private final long dirId;
    private final boolean isReference;
    /**
     * The relative path (related to the snapshot root) of 1) the file/directory
     * where changes have happened, or 2) the source file/dir of a rename op.
     * or 3) target file/dir for a rename op.
     */
    private final byte[][] sourcePath;
    private final byte[][] targetPath;

    public DiffReportListingEntry(long dirId, long fileId, byte[][] sourcePath,
        boolean isReference, byte[][] targetPath) {
      Preconditions.checkNotNull(sourcePath);
      this.dirId = dirId;
      this.fileId = fileId;
      this.sourcePath = sourcePath;
      this.isReference = isReference;
      this.targetPath = targetPath;
    }

    public DiffReportListingEntry(long dirId, long fileId, byte[] sourcePath,
        boolean isReference, byte[] targetpath) {
      Preconditions.checkNotNull(sourcePath);
      this.dirId = dirId;
      this.fileId = fileId;
      this.sourcePath = DFSUtilClient.bytes2byteArray(sourcePath);
      this.isReference = isReference;
      this.targetPath =
          targetpath == null ? null : DFSUtilClient.bytes2byteArray(targetpath);
    }

    public long getDirId() {
      return dirId;
    }

    public long getFileId() {
      return fileId;
    }

    public byte[][] getSourcePath() {
      return sourcePath;
    }

    public byte[][] getTargetPath() {
      return targetPath;
    }

    public boolean isReference() {
      return isReference;
    }
  }

  /** store the starting path to process across RPC's for snapshot diff. */
  private final byte[] lastPath;

  private final int lastIndex;

  private final boolean isFromEarlier;

  /** list of diff. */
  private final List<DiffReportListingEntry> modifyList;

  private final List<DiffReportListingEntry> createList;

  private final List<DiffReportListingEntry> deleteList;

  public SnapshotDiffReportListing() {
    this.modifyList = Collections.emptyList();
    this.createList = Collections.emptyList();
    this.deleteList = Collections.emptyList();
    this.lastPath = DFSUtilClient.string2Bytes("");
    this.lastIndex = -1;
    this.isFromEarlier = false;
  }

  public SnapshotDiffReportListing(byte[] startPath,
      List<DiffReportListingEntry> modifiedEntryList,
      List<DiffReportListingEntry> createdEntryList,
      List<DiffReportListingEntry> deletedEntryList, int index,
      boolean isFromEarlier) {
    this.modifyList = modifiedEntryList;
    this.createList = createdEntryList;
    this.deleteList = deletedEntryList;
    this.lastPath =
        startPath != null ? startPath : DFSUtilClient.string2Bytes("");
    this.lastIndex = index;
    this.isFromEarlier = isFromEarlier;
  }

  public List<DiffReportListingEntry> getModifyList() {
    return modifyList;
  }

  public List<DiffReportListingEntry> getCreateList() {
    return createList;
  }

  public List<DiffReportListingEntry> getDeleteList() {
    return deleteList;
  }

  /**
   * @return {@link #lastPath}
   */
  public byte[] getLastPath() {
    return lastPath;
  }

  public int getLastIndex() {
    return lastIndex;
  }

  public boolean getIsFromEarlier() {
    return isFromEarlier;
  }

}
