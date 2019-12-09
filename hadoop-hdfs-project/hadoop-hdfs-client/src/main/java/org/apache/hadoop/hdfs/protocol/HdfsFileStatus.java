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
import java.io.ObjectInputValidation;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileStatus.AttrFlags;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.io.Writable;

/**
 * HDFS metadata for an entity in the filesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface HdfsFileStatus
    extends Writable, Comparable<Object>, Serializable, ObjectInputValidation {

  byte[] EMPTY_NAME = new byte[0];

  /** Set of features potentially active on an instance. */
  enum Flags {
    HAS_ACL,
    HAS_CRYPT,
    HAS_EC,
    SNAPSHOT_ENABLED
  }

  /**
   * Builder class for HdfsFileStatus instances. Note default values for
   * parameters.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  class Builder {
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
    private LocatedBlocks locations        = null;

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
     * Set the block locations for this entity (default = null).
     * @param locations HDFS locations
     *       (see {@link HdfsLocatedFileStatus#makeQualifiedLocated(URI, Path)})
     * @return This Builder instance
     */
    public Builder locations(LocatedBlocks locations) {
      this.locations = locations;
      return this;
    }

    /**
     * @return An {@link HdfsFileStatus} instance from these parameters.
     */
    public HdfsFileStatus build() {
      if (null == locations && !isdir && null == symlink) {
        return new HdfsNamedFileStatus(length, isdir, replication, blocksize,
            mtime, atime, permission, flags, owner, group, symlink, path,
            fileId, childrenNum, feInfo, storagePolicy, ecPolicy);
      }
      return new HdfsLocatedFileStatus(length, isdir, replication, blocksize,
          mtime, atime, permission, flags, owner, group, symlink, path,
          fileId, childrenNum, feInfo, storagePolicy, ecPolicy, locations);
    }

  }

  ///////////////////
  // HDFS-specific //
  ///////////////////

  /**
   * Inode ID for this entity, if a file.
   * @return inode ID.
   */
  long getFileId();

  /**
   * Get metadata for encryption, if present.
   * @return the {@link FileEncryptionInfo} for this stream, or null if not
   *         encrypted.
   */
  FileEncryptionInfo getFileEncryptionInfo();

  /**
   * Check if the local name is empty.
   * @return true if the name is empty
   */
  default boolean isEmptyLocalName() {
    return getLocalNameInBytes().length == 0;
  }

  /**
   * Get the string representation of the local name.
   * @return the local name in string
   */
  default String getLocalName() {
    return DFSUtilClient.bytes2String(getLocalNameInBytes());
  }

  /**
   * Get the Java UTF8 representation of the local name.
   * @return the local name in java UTF8
   */
  byte[] getLocalNameInBytes();

  /**
   * Get the string representation of the full path name.
   * @param parent the parent path
   * @return the full path in string
   */
  default String getFullName(String parent) {
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
  default Path getFullPath(Path parent) {
    if (isEmptyLocalName()) {
      return parent;
    }

    return new Path(parent, getLocalName());
  }

  /**
   * Opaque referant for the symlink, to be resolved at the client.
   */
  byte[] getSymlinkInBytes();

  /**
   * @return number of children for this inode.
   */
  int getChildrenNum();

  /**
   * Get the erasure coding policy if it's set.
   * @return the erasure coding policy
   */
  ErasureCodingPolicy getErasureCodingPolicy();

  /** @return the storage policy id */
  byte getStoragePolicy();

  /**
   * Resolve the short name of the Path given the URI, parent provided. This
   * FileStatus reference will not contain a valid Path until it is resolved
   * by this method.
   * @param defaultUri FileSystem to fully qualify HDFS path.
   * @param parent Parent path of this element.
   * @return Reference to this instance.
   */
  default FileStatus makeQualified(URI defaultUri, Path parent) {
    // fully-qualify path
    setPath(getFullPath(parent).makeQualified(defaultUri, null));
    return (FileStatus) this; // API compatibility
  }

  ////////////////////////////
  // FileStatus "overrides" //
  ////////////////////////////

  /**
   * See {@link FileStatus#getPath()}.
   */
  Path getPath();
  /**
   * See {@link FileStatus#setPath(Path)}.
   */
  void setPath(Path p);
  /**
   * See {@link FileStatus#getLen()}.
   */
  long getLen();
  /**
   * See {@link FileStatus#isFile()}.
   */
  boolean isFile();
  /**
   * See {@link FileStatus#isDirectory()}.
   */
  boolean isDirectory();
  /**
   * See {@link FileStatus#isDir()}.
   */
  boolean isDir();
  /**
   * See {@link FileStatus#isSymlink()}.
   */
  boolean isSymlink();
  /**
   * See {@link FileStatus#getBlockSize()}.
   */
  long getBlockSize();
  /**
   * See {@link FileStatus#getReplication()}.
   */
  short getReplication();
  /**
   * See {@link FileStatus#getModificationTime()}.
   */
  long getModificationTime();
  /**
   * See {@link FileStatus#getAccessTime()}.
   */
  long getAccessTime();
  /**
   * See {@link FileStatus#getPermission()}.
   */
  FsPermission getPermission();
  /**
   * See {@link FileStatus#setPermission(FsPermission)}.
   */
  void setPermission(FsPermission permission);
  /**
   * See {@link FileStatus#getOwner()}.
   */
  String getOwner();
  /**
   * See {@link FileStatus#setOwner(String)}.
   */
  void setOwner(String owner);
  /**
   * See {@link FileStatus#getGroup()}.
   */
  String getGroup();
  /**
   * See {@link FileStatus#setGroup(String)}.
   */
  void setGroup(String group);
  /**
   * See {@link FileStatus#hasAcl()}.
   */
  boolean hasAcl();
  /**
   * See {@link FileStatus#isEncrypted()}.
   */
  boolean isEncrypted();
  /**
   * See {@link FileStatus#isErasureCoded()}.
   */
  boolean isErasureCoded();
  /**
   * See {@link FileStatus#isSnapshotEnabled()}.
   */
  boolean isSnapshotEnabled();
  /**
   * See {@link FileStatus#getSymlink()}.
   */
  Path getSymlink() throws IOException;
  /**
   * See {@link FileStatus#setSymlink(Path sym)}.
   */
  void setSymlink(Path sym);
  /**
   * See {@link FileStatus#compareTo(FileStatus)}.
   */
  int compareTo(FileStatus stat);

  /**
   * Set redundant flags for compatibility with existing applications.
   */
  static FsPermission convert(boolean isdir, boolean symlink,
                              FsPermission p, Set<Flags> f) {
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

  static Set<AttrFlags> convert(Set<Flags> flags) {
    if (flags.isEmpty()) {
      return FileStatus.NONE;
    }
    EnumSet<AttrFlags> attr = EnumSet.noneOf(AttrFlags.class);
    if (flags.contains(Flags.HAS_ACL)) {
      attr.add(AttrFlags.HAS_ACL);
    }
    if (flags.contains(Flags.HAS_EC)) {
      attr.add(AttrFlags.HAS_EC);
    }
    if (flags.contains(Flags.HAS_CRYPT)) {
      attr.add(AttrFlags.HAS_CRYPT);
    }
    if (flags.contains(Flags.SNAPSHOT_ENABLED)) {
      attr.add(AttrFlags.SNAPSHOT_ENABLED);
    }
    return attr;
  }

}
