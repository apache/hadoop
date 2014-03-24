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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable.SnapshotDiffInfo;

/**
 * This class represents to end users the difference between two snapshots of 
 * the same directory, or the difference between a snapshot of the directory and
 * its current state. Instead of capturing all the details of the diff, which 
 * is stored in {@link SnapshotDiffInfo}, this class only lists where the 
 * changes happened and their types.
 */
public class SnapshotDiffReport {
  private final static String LINE_SEPARATOR = System.getProperty(
      "line.separator", "\n");

  /**
   * Types of the difference, which include CREATE, MODIFY, DELETE, and RENAME.
   * Each type has a label for representation: +/M/-/R represent CREATE, MODIFY,
   * DELETE, and RENAME respectively.
   */
  public enum DiffType {
    CREATE("+"),     
    MODIFY("M"),    
    DELETE("-"), 
    RENAME("R");
    
    private final String label;
    
    private DiffType(String label) {
      this.label = label;
    }
    
    public String getLabel() {
      return label;
    }
    
    public static DiffType getTypeFromLabel(String label) {
      if (label.equals(CREATE.getLabel())) {
        return CREATE;
      } else if (label.equals(MODIFY.getLabel())) {
        return MODIFY;
      } else if (label.equals(DELETE.getLabel())) {
        return DELETE;
      } else if (label.equals(RENAME.getLabel())) {
        return RENAME;
      }
      return null;
    }
  };
  
  /**
   * Representing the full path and diff type of a file/directory where changes
   * have happened.
   */
  public static class DiffReportEntry {
    /** The type of the difference. */
    private final DiffType type;
    /**
     * The relative path (related to the snapshot root) of the file/directory
     * where changes have happened
     */
    private final byte[] relativePath;

    public DiffReportEntry(DiffType type, byte[] path) {
      this.type = type;
      this.relativePath = path;
    }
    
    public DiffReportEntry(DiffType type, byte[][] pathComponents) {
      this.type = type;
      this.relativePath = DFSUtil.byteArray2bytes(pathComponents);
    }
    
    @Override
    public String toString() {
      return type.getLabel() + "\t" + getRelativePathString();
    }
    
    public DiffType getType() {
      return type;
    }

    public String getRelativePathString() {
      String path = DFSUtil.bytes2String(relativePath);
      if (path.isEmpty()) {
        return Path.CUR_DIR;
      } else {
        return Path.CUR_DIR + Path.SEPARATOR + path;
      }
    }

    public byte[] getRelativePath() {
      return relativePath;
    }
    
    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } 
      if (other != null && other instanceof DiffReportEntry) {
        DiffReportEntry entry = (DiffReportEntry) other;
        return type.equals(entry.getType())
            && Arrays.equals(relativePath, entry.getRelativePath());
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return Arrays.hashCode(relativePath);
    }
  }
  
  /** snapshot root full path */
  private final String snapshotRoot;

  /** start point of the diff */
  private final String fromSnapshot;
  
  /** end point of the diff */
  private final String toSnapshot;
  
  /** list of diff */
  private final List<DiffReportEntry> diffList;
  
  public SnapshotDiffReport(String snapshotRoot, String fromSnapshot,
      String toSnapshot, List<DiffReportEntry> entryList) {
    this.snapshotRoot = snapshotRoot;
    this.fromSnapshot = fromSnapshot;
    this.toSnapshot = toSnapshot;
    this.diffList = entryList != null ? entryList : Collections
        .<DiffReportEntry> emptyList();
  }
  
  /** @return {@link #snapshotRoot}*/
  public String getSnapshotRoot() {
    return snapshotRoot;
  }

  /** @return {@link #fromSnapshot} */
  public String getFromSnapshot() {
    return fromSnapshot;
  }

  /** @return {@link #toSnapshot} */
  public String getLaterSnapshotName() {
    return toSnapshot;
  }
  
  /** @return {@link #diffList} */
  public List<DiffReportEntry> getDiffList() {
    return diffList;
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    String from = fromSnapshot == null || fromSnapshot.isEmpty() ? 
        "current directory" : "snapshot " + fromSnapshot;
    String to = toSnapshot == null || toSnapshot.isEmpty() ? "current directory"
        : "snapshot " + toSnapshot;
    str.append("Difference between " + from + " and " + to
        + " under directory " + snapshotRoot + ":" + LINE_SEPARATOR);
    for (DiffReportEntry entry : diffList) {
      str.append(entry.toString() + LINE_SEPARATOR);
    }
    return str.toString();
  }
}
