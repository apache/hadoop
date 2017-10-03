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

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;

/** Interface that represents the over the wire information for a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsFileStatus extends FileStatus {

  private static final long serialVersionUID = 0x126eb82a;

  // local name of the inode that's encoded in java UTF8
  private byte[] uPath;
  private byte[] uSymlink; // symlink target encoded in java UTF8/null
  private final long fileId;
  private final FileEncryptionInfo feInfo;
  private final ErasureCodingPolicy ecPolicy;

  // Used by dir, not including dot and dotdot. Always zero for a regular file.
  private final int childrenNum;
  private final byte storagePolicy;

  public static final byte[] EMPTY_NAME = new byte[0];

  /**
   * Set of features potentially active on an instance.
   */
  public enum Flags {
    HAS_ACL,
    HAS_CRYPT,
    HAS_EC,
    SNAPSHOT_ENABLED
  }
  private final EnumSet<Flags> flags;

  /**
   * Constructor.
   * @param length            the number of bytes the file has
   * @param isdir             if the path is a directory
   * @param block_replication the replication factor
   * @param blocksize         the block size
   * @param modification_time modification time
   * @param access_time access time
   * @param permission permission
   * @param owner the owner of the path
   * @param group the group of the path
   * @param symlink symlink target encoded in java UTF8 or null
   * @param path the local name in java UTF8 encoding the same as that in-memory
   * @param fileId the file id
   * @param childrenNum the number of children. Used by directory.
   * @param feInfo the file's encryption info
   * @param storagePolicy ID which specifies storage policy
   * @param ecPolicy the erasure coding policy
   */
  public HdfsFileStatus(long length, boolean isdir, int block_replication,
                        long blocksize, long modification_time,
                        long access_time, FsPermission permission,
                        EnumSet<Flags> flags, String owner, String group,
                        byte[] symlink, byte[] path, long fileId,
                        int childrenNum, FileEncryptionInfo feInfo,
                        byte storagePolicy, ErasureCodingPolicy ecPolicy) {
    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, convert(isdir, symlink != null, permission, flags),
        owner, group, null, null);
    this.flags = flags;
    this.uSymlink = symlink;
    this.uPath = path;
    this.fileId = fileId;
    this.childrenNum = childrenNum;
    this.feInfo = feInfo;
    this.storagePolicy = storagePolicy;
    this.ecPolicy = ecPolicy;
  }

  /**
   * Set redundant flags for compatibility with existing applications.
   */
  protected static FsPermission convert(boolean isdir, boolean symlink,
      FsPermission p, EnumSet<Flags> f) {
    if (p instanceof FsPermissionExtension) {
      // verify flags are set consistently
      assert p.getAclBit() == f.contains(HdfsFileStatus.Flags.HAS_ACL);
      assert p.getEncryptedBit() == f.contains(HdfsFileStatus.Flags.HAS_CRYPT);
      assert p.getErasureCodedBit() == f.contains(HdfsFileStatus.Flags.HAS_EC);
      return p;
    }
    if (null == p) {
      if (isdir) {
        p = FsPermission.getDirDefault();
      } else if (symlink) {
        p = FsPermission.getDefault();
      } else {
        p = FsPermission.getFileDefault();
      }
    }
    return new FsPermissionExtension(p, f.contains(Flags.HAS_ACL),
        f.contains(Flags.HAS_CRYPT), f.contains(Flags.HAS_EC));
  }

  @Override
  public boolean isSymlink() {
    return uSymlink != null;
  }

  @Override
  public boolean hasAcl() {
    return flags.contains(Flags.HAS_ACL);
  }

  @Override
  public boolean isEncrypted() {
    return flags.contains(Flags.HAS_CRYPT);
  }

  @Override
  public boolean isErasureCoded() {
    return flags.contains(Flags.HAS_EC);
  }

  /**
   * Check if the local name is empty.
   * @return true if the name is empty
   */
  public final boolean isEmptyLocalName() {
    return uPath.length == 0;
  }

  /**
   * Get the string representation of the local name.
   * @return the local name in string
   */
  public final String getLocalName() {
    return DFSUtilClient.bytes2String(uPath);
  }

  /**
   * Get the Java UTF8 representation of the local name.
   * @return the local name in java UTF8
   */
  public final byte[] getLocalNameInBytes() {
    return uPath;
  }

  /**
   * Get the string representation of the full path name.
   * @param parent the parent path
   * @return the full path in string
   */
  public final String getFullName(final String parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    StringBuilder fullName = new StringBuilder(parent);
    if (!parent.endsWith(Path.SEPARATOR)) {
      fullName.append(Path.SEPARATOR);
    }
    fullName.append(getLocalName());
    return fullName.toString();
  }

  /**
   * Get the full path.
   * @param parent the parent path
   * @return the full path
   */
  public final Path getFullPath(final Path parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    return new Path(parent, getLocalName());
  }

  @Override
  public Path getSymlink() throws IOException {
    if (isSymlink()) {
      return new Path(DFSUtilClient.bytes2String(uSymlink));
    }
    throw new IOException("Path " + getPath() + " is not a symbolic link");
  }

  @Override
  public void setSymlink(Path sym) {
    uSymlink = DFSUtilClient.string2Bytes(sym.toString());
  }

  /**
   * Opaque referant for the symlink, to be resolved at the client.
   */
  public final byte[] getSymlinkInBytes() {
    return uSymlink;
  }

  public final long getFileId() {
    return fileId;
  }

  public final FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  /**
   * Get the erasure coding policy if it's set.
   * @return the erasure coding policy
   */
  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  public final int getChildrenNum() {
    return childrenNum;
  }

  /** @return the storage policy id */
  public final byte getStoragePolicy() {
    return storagePolicy;
  }

  /**
   * Check if directory is Snapshot enabled or not.
   *
   * @return true if directory is snapshot enabled
   */
  public boolean isSnapshotEnabled() {
    return flags.contains(Flags.SNAPSHOT_ENABLED);
  }

  @Override
  public boolean equals(Object o) {
    // satisfy findbugs
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // satisfy findbugs
    return super.hashCode();
  }

  /**
   * Resolve the short name of the Path given the URI, parent provided. This
   * FileStatus reference will not contain a valid Path until it is resolved
   * by this method.
   * @param defaultUri FileSystem to fully qualify HDFS path.
   * @param parent Parent path of this element.
   * @return Reference to this instance.
   */
  public final FileStatus makeQualified(URI defaultUri, Path parent) {
    // fully-qualify path
    setPath(getFullPath(parent).makeQualified(defaultUri, null));
    return this; // API compatibility
  }

}
