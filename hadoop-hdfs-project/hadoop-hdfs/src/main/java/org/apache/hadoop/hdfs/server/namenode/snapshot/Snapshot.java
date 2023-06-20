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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SNAPSHOT_DELETED;

/** Snapshot of a sub-tree in the namesystem. */
@InterfaceAudience.Private
public class Snapshot implements Comparable<byte[]> {
  /**
   * This id is used to indicate the current state (vs. snapshots)
   */
  public static final int CURRENT_STATE_ID = Integer.MAX_VALUE - 1;
  public static final int NO_SNAPSHOT_ID = -1;
  
  /**
   * The pattern for generating the default snapshot name.
   * E.g. s20130412-151029.033
   */
  private static final String DEFAULT_SNAPSHOT_NAME_PATTERN = "'s'yyyyMMdd-HHmmss.SSS";
  
  public static String generateDefaultSnapshotName() {
    return new SimpleDateFormat(DEFAULT_SNAPSHOT_NAME_PATTERN).format(new Date());
  }

  public static String generateDeletedSnapshotName(Snapshot s) {
    return getSnapshotName(s) + "#" + s.getId();
  }

  public static String getSnapshotPath(String snapshottableDir,
      String snapshotRelativePath) {
    final StringBuilder b = new StringBuilder(snapshottableDir);
    if (b.charAt(b.length() - 1) != Path.SEPARATOR_CHAR) {
      b.append(Path.SEPARATOR);
    }
    return b.append(HdfsConstants.DOT_SNAPSHOT_DIR)
        .append(Path.SEPARATOR)
        .append(snapshotRelativePath)
        .toString();
  }
  
  /**
   * Get the name of the given snapshot.
   * @param s The given snapshot.
   * @return The name of the snapshot, or an empty string if {@code s} is null
   */
  static String getSnapshotName(Snapshot s) {
    return s != null ? s.getRoot().getLocalName() : "";
  }
  
  public static int getSnapshotId(Snapshot s) {
    return s == null ? CURRENT_STATE_ID : s.getId();
  }

  public static String getSnapshotString(int snapshot) {
    return snapshot == CURRENT_STATE_ID? "<CURRENT_STATE>"
        : snapshot == NO_SNAPSHOT_ID? "<NO_SNAPSHOT>"
        : "Snapshot #" + snapshot;
  }

  /**
   * Compare snapshot with IDs, where null indicates the current status thus
   * is greater than any non-null snapshot.
   */
  public static final Comparator<Snapshot> ID_COMPARATOR
      = new Comparator<Snapshot>() {
    @Override
    public int compare(Snapshot left, Snapshot right) {
      return ID_INTEGER_COMPARATOR.compare(Snapshot.getSnapshotId(left),
          Snapshot.getSnapshotId(right));
    }
  };

  /**
   * Compare snapshot with IDs, where null indicates the current status thus
   * is greater than any non-null ID.
   */
  public static final Comparator<Integer> ID_INTEGER_COMPARATOR
      = new Comparator<Integer>() {
    @Override
    public int compare(Integer left, Integer right) {
      // Snapshot.CURRENT_STATE_ID means the current state, thus should be the 
      // largest
      return left - right;
    }
  };

  /**
   * Find the latest snapshot that 1) covers the given inode (which means the
   * snapshot was either taken on the inode or taken on an ancestor of the
   * inode), and 2) was taken before the given snapshot (if the given snapshot 
   * is not null).
   * 
   * @param inode the given inode that the returned snapshot needs to cover
   * @param anchor the returned snapshot should be taken before this given id.
   * @return id of the latest snapshot that covers the given inode and was taken 
   *         before the the given snapshot (if it is not null).
   */
  public static int findLatestSnapshot(INode inode, final int anchor) {
    int latest = NO_SNAPSHOT_ID;
    for(; inode != null; inode = inode.getParent()) {
      if (inode.isDirectory()) {
        final INodeDirectory dir = inode.asDirectory();
        if (dir.isWithSnapshot()) {
          latest = dir.getDiffs().updatePrior(anchor, latest);
        }
      }
    }
    return latest;
  }
  
  static Snapshot read(DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    final int snapshotId = in.readInt();
    final INode root = loader.loadINodeWithLocalName(false, in, false);
    return new Snapshot(snapshotId, root.asDirectory(), null);
  }

  /** The root directory of the snapshot. */
  static public class Root extends INodeDirectory {
    Root(INodeDirectory other) {
      // Always preserve ACL, XAttr and Quota.
      super(other, false,
          Arrays.stream(other.getFeatures()).filter(feature ->
              feature instanceof AclFeature
                  || feature instanceof XAttrFeature
                  || feature instanceof DirectoryWithQuotaFeature
          ).map(feature -> {
            if (feature instanceof DirectoryWithQuotaFeature) {
              // Return copy if feature is quota because a ref could be updated
              final QuotaCounts quota =
                  ((DirectoryWithQuotaFeature) feature).getSpaceAllowed();
              return new DirectoryWithQuotaFeature.Builder()
                  .nameSpaceQuota(quota.getNameSpace())
                  .storageSpaceQuota(quota.getStorageSpace())
                  .typeQuotas(quota.getTypeSpaces())
                  .build();
            } else {
              return feature;
            }
          }).toArray(Feature[]::new));
    }

    boolean isMarkedAsDeleted() {
      final XAttrFeature f = getXAttrFeature();
      return f != null && f.getXAttr(XATTR_SNAPSHOT_DELETED) != null;
    }

    @Override
    public ReadOnlyList<INode> getChildrenList(int snapshotId) {
      return getParent().getChildrenList(snapshotId);
    }

    @Override
    public INode getChild(byte[] name, int snapshotId) {
      return getParent().getChild(name, snapshotId);
    }

    @Override
    public ContentSummaryComputationContext computeContentSummary(
        int snapshotId, ContentSummaryComputationContext summary)
        throws AccessControlException {
      return computeDirectoryContentSummary(summary, snapshotId);
    }

    @Override
    public boolean metadataEquals(INodeDirectoryAttributes other) {
      return other != null && getQuotaCounts().equals(other.getQuotaCounts())
          && getPermissionLong() == other.getPermissionLong()
          // Acl feature maintains a reference counted map, thereby
          // every snapshot copy should point to the same Acl object unless
          // there is no change in acl values.
          // Reference equals is hence intentional here.
          && getAclFeature() == other.getAclFeature()
          && Objects.equals(getXAttrFeature(), other.getXAttrFeature());
    }

    @Override
    public String getFullPathName() {
      return getSnapshotPath(getParent().getFullPathName(), getLocalName());
    }

    /**
     * Get the full path name of the root directory of this snapshot.
     * @return full path to the root directory of the snapshot
     */
    public String getRootFullPathName() {
      return getParent().getFullPathName();
    }
  }

  /** Snapshot ID. */
  private final int id;
  /** The root directory of the snapshot. */
  private final Root root;

  Snapshot(int id, String name, INodeDirectory dir) {
    this(id, dir, dir);
    this.root.setLocalName(DFSUtil.string2Bytes(name));
  }

  Snapshot(int id, INodeDirectory dir, INodeDirectory parent) {
    this.id = id;
    this.root = new Root(dir);
    this.root.setParent(parent);
  }
  
  public int getId() {
    return id;
  }

  /** @return the root directory of the snapshot. */
  public Root getRoot() {
    return root;
  }

  @Override
  public int compareTo(byte[] bytes) {
    return root.compareTo(bytes);
  }
  
  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (!(that instanceof Snapshot)) {
      return false;
    }
    return this.id == ((Snapshot)that).id;
  }
  
  @Override
  public int hashCode() {
    return id;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "." + root.getLocalName() + "(id=" + id + ")";
  }

  /** Serialize the fields to out */
  void write(DataOutput out) throws IOException {
    out.writeInt(id);
    // write root
    FSImageSerialization.writeINodeDirectory(root, out);
  }
}
