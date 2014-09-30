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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** Interface that represents the client side information for a file.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileStatus implements Writable, Comparable {

  private Path path;
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private FsPermission permission;
  private String owner;
  private String group;
  private Path symlink;
  
  public FileStatus() { this(0, false, 0, 0, 0, 0, null, null, null, null); }
  
  //We should deprecate this soon?
  public FileStatus(long length, boolean isdir, int block_replication,
                    long blocksize, long modification_time, Path path) {

    this(length, isdir, block_replication, blocksize, modification_time,
         0, null, null, null, path);
  }

  /**
   * Constructor for file systems on which symbolic links are not supported
   */
  public FileStatus(long length, boolean isdir,
                    int block_replication,
                    long blocksize, long modification_time, long access_time,
                    FsPermission permission, String owner, String group, 
                    Path path) {
    this(length, isdir, block_replication, blocksize, modification_time,
         access_time, permission, owner, group, null, path);
  }

  public FileStatus(long length, boolean isdir,
                    int block_replication,
                    long blocksize, long modification_time, long access_time,
                    FsPermission permission, String owner, String group, 
                    Path symlink,
                    Path path) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short)block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    if (permission != null) {
      this.permission = permission;
    } else if (isdir) {
      this.permission = FsPermission.getDirDefault();
    } else if (symlink!=null) {
      this.permission = FsPermission.getDefault();
    } else {
      this.permission = FsPermission.getFileDefault();
    }
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
    // The variables isdir and symlink indicate the type:
    // 1. isdir implies directory, in which case symlink must be null.
    // 2. !isdir implies a file or symlink, symlink != null implies a
    //    symlink, otherwise it's a file.
    assert (isdir && symlink == null) || !isdir;
  }

  /**
   * Copy constructor.
   *
   * @param other FileStatus to copy
   */
  public FileStatus(FileStatus other) throws IOException {
    // It's important to call the getters here instead of directly accessing the
    // members.  Subclasses like ViewFsFileStatus can override the getters.
    this(other.getLen(), other.isDirectory(), other.getReplication(),
      other.getBlockSize(), other.getModificationTime(), other.getAccessTime(),
      other.getPermission(), other.getOwner(), other.getGroup(),
      (other.isSymlink() ? other.getSymlink() : null),
      other.getPath());
  }

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  public long getLen() {
    return length;
  }

  /**
   * Is this a file?
   * @return true if this is a file
   */
  public boolean isFile() {
    return !isdir && !isSymlink();
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public boolean isDirectory() {
    return isdir;
  }
  
  /**
   * Old interface, instead use the explicit {@link FileStatus#isFile()}, 
   * {@link FileStatus#isDirectory()}, and {@link FileStatus#isSymlink()} 
   * @return true if this is a directory.
   * @deprecated Use {@link FileStatus#isFile()},  
   * {@link FileStatus#isDirectory()}, and {@link FileStatus#isSymlink()} 
   * instead.
   */
  @Deprecated
  public boolean isDir() {
    return isdir;
  }
  
  /**
   * Is this a symbolic link?
   * @return true if this is a symbolic link
   */
  public boolean isSymlink() {
    return symlink != null;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getAccessTime() {
    return access_time;
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion. If a filesystem does not have a notion of permissions
   *         or if permissions could not be determined, then default 
   *         permissions equivalent of "rwxrwxrwx" is returned.
   */
  public FsPermission getPermission() {
    return permission;
  }

  /**
   * Tell whether the underlying file or directory is encrypted or not.
   *
   * @return true if the underlying file is encrypted.
   */
  public boolean isEncrypted() {
    return permission.getEncryptedBit();
  }
  
  /**
   * Get the owner of the file.
   * @return owner of the file. The string could be empty if there is no
   *         notion of owner of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public String getOwner() {
    return owner;
  }
  
  /**
   * Get the group associated with the file.
   * @return group for the file. The string could be empty if there is no
   *         notion of group of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public String getGroup() {
    return group;
  }
  
  public Path getPath() {
    return path;
  }
  
  public void setPath(final Path p) {
    path = p;
  }

  /* These are provided so that these values could be loaded lazily 
   * by a filesystem (e.g. local file system).
   */
  
  /**
   * Sets permission.
   * @param permission if permission is null, default value is set
   */
  protected void setPermission(FsPermission permission) {
    this.permission = (permission == null) ? 
                      FsPermission.getFileDefault() : permission;
  }
  
  /**
   * Sets owner.
   * @param owner if it is null, default value is set
   */  
  protected void setOwner(String owner) {
    this.owner = (owner == null) ? "" : owner;
  }
  
  /**
   * Sets group.
   * @param group if it is null, default value is set
   */  
  protected void setGroup(String group) {
    this.group = (group == null) ? "" :  group;
  }

  /**
   * @return The contents of the symbolic link.
   */
  public Path getSymlink() throws IOException {
    if (!isSymlink()) {
      throw new IOException("Path " + path + " is not a symbolic link");
    }
    return symlink;
  }

  public void setSymlink(final Path p) {
    symlink = p;
  }
  
  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString(), Text.DEFAULT_MAX_LEN);
    out.writeLong(getLen());
    out.writeBoolean(isDirectory());
    out.writeShort(getReplication());
    out.writeLong(getBlockSize());
    out.writeLong(getModificationTime());
    out.writeLong(getAccessTime());
    getPermission().write(out);
    Text.writeString(out, getOwner(), Text.DEFAULT_MAX_LEN);
    Text.writeString(out, getGroup(), Text.DEFAULT_MAX_LEN);
    out.writeBoolean(isSymlink());
    if (isSymlink()) {
      Text.writeString(out, getSymlink().toString(), Text.DEFAULT_MAX_LEN);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String strPath = Text.readString(in, Text.DEFAULT_MAX_LEN);
    this.path = new Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.block_replication = in.readShort();
    blocksize = in.readLong();
    modification_time = in.readLong();
    access_time = in.readLong();
    permission.readFields(in);
    owner = Text.readString(in, Text.DEFAULT_MAX_LEN);
    group = Text.readString(in, Text.DEFAULT_MAX_LEN);
    if (in.readBoolean()) {
      this.symlink = new Path(Text.readString(in, Text.DEFAULT_MAX_LEN));
    } else {
      this.symlink = null;
    }
  }

  /**
   * Compare this object to another object
   * 
   * @param   o the object to be compared.
   * @return  a negative integer, zero, or a positive integer as this object
   *   is less than, equal to, or greater than the specified object.
   * 
   * @throws ClassCastException if the specified object's is not of 
   *         type FileStatus
   */
  @Override
  public int compareTo(Object o) {
    FileStatus other = (FileStatus)o;
    return this.getPath().compareTo(other.getPath());
  }
  
  /** Compare if this object is equal to another object
   * @param   o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileStatus)) {
      return false;
    }
    FileStatus other = (FileStatus)o;
    return this.getPath().equals(other.getPath());
  }
  
  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  @Override
  public int hashCode() {
    return getPath().hashCode();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()); 
    sb.append("{");
    sb.append("path=" + path);
    sb.append("; isDirectory=" + isdir);
    if(!isDirectory()){
      sb.append("; length=" + length);
      sb.append("; replication=" + block_replication);
      sb.append("; blocksize=" + blocksize);
    }
    sb.append("; modification_time=" + modification_time);
    sb.append("; access_time=" + access_time);
    sb.append("; owner=" + owner);
    sb.append("; group=" + group);
    sb.append("; permission=" + permission);
    sb.append("; isSymlink=" + isSymlink());
    if(isSymlink()) {
      sb.append("; symlink=" + symlink);
    }
    sb.append("}");
    return sb.toString();
  }
}
