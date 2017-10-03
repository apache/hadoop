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
import java.util.Arrays;
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
   * @param length the number of bytes the file has
   * @param isdir if the path is a directory
   * @param replication the replication factor
   * @param blocksize the block size
   * @param mtime modification time
   * @param atime access time
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
  protected HdfsFileStatus(long length, boolean isdir, int replication,
                         long blocksize, long mtime, long atime,
                         FsPermission permission, EnumSet<Flags> flags,
                         String owner, String group,
                         byte[] symlink, byte[] path, long fileId,
                         int childrenNum, FileEncryptionInfo feInfo,
                         byte storagePolicy, ErasureCodingPolicy ecPolicy) {
    super(length, isdir, replication, blocksize, mtime,
        atime, convert(isdir, symlink != null, permission, flags),
        owner, group, null, null,
        flags.contains(Flags.HAS_ACL), flags.contains(Flags.HAS_CRYPT),
        flags.contains(Flags.HAS_EC));
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

  /**
   * Builder class for HdfsFileStatus instances. Note default values for
   * parameters.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Builder {
    // Changing default values will affect cases where values are not
    // specified. Be careful!
    private long length                    = 0L;
    private boolean isdir                  = false;
    private int replication                = 0;
    private long blocksize                 = 0L;
    private long mtime                     = 0L;
    private long atime                     = 0L;
    private FsPermission permission        = null;
    private EnumSet<Flags> flags           = EnumSet.noneOf(Flags.class);
    private String owner                   = null;
    private String group                   = null;
    private byte[] symlink                 = null;
    private byte[] path                    = EMPTY_NAME;
    private long fileId                    = -1L;
    private int childrenNum                = 0;
    private FileEncryptionInfo feInfo      = null;
    private byte storagePolicy             =
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    private ErasureCodingPolicy ecPolicy   = null;

    /**
     * Set the length of the entity (default = 0).
     * @param length Entity length
     * @return This Builder instance
     */
    public Builder length(long length) {
      this.length = length;
      return this;
    }

    /**
     * Set the isDir flag for the entity (default = false).
     * @param isdir True if the referent is a directory.
     * @return This Builder instance
     */
    public Builder isdir(boolean isdir) {
      this.isdir = isdir;
      return this;
    }

    /**
     * Set the replication of this entity (default = 0).
     * @param replication Number of replicas
     * @return This Builder instance
     */
    public Builder replication(int replication) {
      this.replication = replication;
      return this;
    }

    /**
     * Set the blocksize of this entity (default = 0).
     * @param blocksize Target, default blocksize
     * @return This Builder instance
     */
    public Builder blocksize(long blocksize) {
      this.blocksize = blocksize;
      return this;
    }

    /**
     * Set the modification time of this entity (default = 0).
     * @param mtime Last modified time
     * @return This Builder instance
     */
    public Builder mtime(long mtime) {
      this.mtime = mtime;
      return this;
    }

    /**
     * Set the access time of this entity (default = 0).
     * @param atime Last accessed time
     * @return This Builder instance
     */
    public Builder atime(long atime) {
      this.atime = atime;
      return this;
    }

    /**
     * Set the permission mask of this entity (default = null).
     * @param permission Permission bitmask
     * @return This Builder instance
     */
    public Builder perm(FsPermission permission) {
      this.permission = permission;
      return this;
    }

    /**
     * Set {@link Flags} for this entity
     * (default = {@link EnumSet#noneOf(Class)}).
     * @param flags Flags
     * @return This builder instance
     */
    public Builder flags(EnumSet<Flags> flags) {
      this.flags = flags;
      return this;
    }

    /**
     * Set the owner for this entity (default = null).
     * @param owner Owner
     * @return This Builder instance
     */
    public Builder owner(String owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Set the group for this entity (default = null).
     * @param group Group
     * @return This Builder instance
     */
    public Builder group(String group) {
      this.group = group;
      return this;
    }

    /**
     * Set symlink bytes for this entity (default = null).
     * @param symlink Symlink bytes (see
     *                {@link DFSUtilClient#bytes2String(byte[])})
     * @return This Builder instance
     */
    public Builder symlink(byte[] symlink) {
      this.symlink = null == symlink
          ? null
          : Arrays.copyOf(symlink, symlink.length);
      return this;
    }

    /**
     * Set path bytes for this entity (default = {@link #EMPTY_NAME}).
     * @param path Path bytes (see {@link #makeQualified(URI, Path)}).
     * @return This Builder instance
     */
    public Builder path(byte[] path) {
      this.path = null == path
          ? null
          : Arrays.copyOf(path, path.length);
      return this;
    }

    /**
     * Set the fileId for this entity (default = -1).
     * @param fileId FileId
     * @return This Builder instance
     */
    public Builder fileId(long fileId) {
      this.fileId = fileId;
      return this;
    }

    /**
     * Set the number of children for this entity (default = 0).
     * @param childrenNum Number of children
     * @return This Builder instance
     */
    public Builder children(int childrenNum) {
      this.childrenNum = childrenNum;
      return this;
    }

    /**
     * Set the encryption info for this entity (default = null).
     * @param feInfo Encryption info
     * @return This Builder instance
     */
    public Builder feInfo(FileEncryptionInfo feInfo) {
      this.feInfo = feInfo;
      return this;
    }

    /**
     * Set the storage policy for this entity
     * (default = {@link HdfsConstants#BLOCK_STORAGE_POLICY_ID_UNSPECIFIED}).
     * @param storagePolicy Storage policy
     * @return This Builder instance
     */
    public Builder storagePolicy(byte storagePolicy) {
      this.storagePolicy = storagePolicy;
      return this;
    }

    /**
     * Set the erasure coding policy for this entity (default = null).
     * @param ecPolicy Erasure coding policy
     * @return This Builder instance
     */
    public Builder ecPolicy(ErasureCodingPolicy ecPolicy) {
      this.ecPolicy = ecPolicy;
      return this;
    }

    /**
     * @return An {@link HdfsFileStatus} instance from these parameters.
     */
    public HdfsFileStatus build() {
      return new HdfsFileStatus(length, isdir, replication, blocksize,
          mtime, atime, permission, flags, owner, group, symlink, path, fileId,
          childrenNum, feInfo, storagePolicy, ecPolicy);
    }
  }

}
