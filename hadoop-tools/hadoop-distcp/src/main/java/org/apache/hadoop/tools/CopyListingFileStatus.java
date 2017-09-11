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
package org.apache.hadoop.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * CopyListingFileStatus is a view of {@link FileStatus}, recording additional
 * data members useful to distcp.
 */
@InterfaceAudience.Private
public final class CopyListingFileStatus implements Writable {

  private static final byte NO_ACL_ENTRIES = -1;
  private static final int NO_XATTRS = -1;

  // FileStatus fields
  private Path path;
  private long length;
  private boolean isdir;
  private short blockReplication;
  private long blocksize;
  private long modificationTime;
  private long accessTime;
  private FsPermission permission;
  private String owner;
  private String group;

  // Retain static arrays of enum values to prevent repeated allocation of new
  // arrays during deserialization.
  private static final AclEntryType[] ACL_ENTRY_TYPES = AclEntryType.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPES = AclEntryScope.values();
  private static final FsAction[] FS_ACTIONS = FsAction.values();

  private List<AclEntry> aclEntries;
  private Map<String, byte[]> xAttrs;

  // <chunkOffset, chunkLength> represents the offset and length of a file
  // chunk in number of bytes.
  // used when splitting a large file to chunks to copy in parallel.
  // If a file is not large enough to split, chunkOffset would be 0 and
  // chunkLength would be the length of the file.
  private long chunkOffset = 0;
  private long chunkLength = Long.MAX_VALUE;

  /**
   * Default constructor.
   */
  public CopyListingFileStatus() {
    this(0, false, 0, 0, 0, 0, null, null, null, null);
  }

  /**
   * Creates a new CopyListingFileStatus by copying the members of the given
   * FileStatus.
   *
   * @param fileStatus FileStatus to copy
   */
  public CopyListingFileStatus(FileStatus fileStatus) {
    this(fileStatus.getLen(), fileStatus.isDirectory(),
        fileStatus.getReplication(), fileStatus.getBlockSize(),
        fileStatus.getModificationTime(), fileStatus.getAccessTime(),
        fileStatus.getPermission(), fileStatus.getOwner(),
        fileStatus.getGroup(),
        fileStatus.getPath());
  }

  public CopyListingFileStatus(FileStatus fileStatus,
      long chunkOffset, long chunkLength) {
    this(fileStatus.getLen(), fileStatus.isDirectory(),
        fileStatus.getReplication(), fileStatus.getBlockSize(),
        fileStatus.getModificationTime(), fileStatus.getAccessTime(),
        fileStatus.getPermission(), fileStatus.getOwner(),
        fileStatus.getGroup(),
        fileStatus.getPath());
    this.chunkOffset = chunkOffset;
    this.chunkLength = chunkLength;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public CopyListingFileStatus(long length, boolean isdir,
      int blockReplication, long blocksize, long modificationTime,
      long accessTime, FsPermission permission, String owner, String group,
      Path path) {
    this(length, isdir, blockReplication, blocksize, modificationTime,
        accessTime, permission, owner, group, path, 0, Long.MAX_VALUE);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public CopyListingFileStatus(long length, boolean isdir,
      int blockReplication, long blocksize, long modificationTime,
      long accessTime, FsPermission permission, String owner, String group,
      Path path, long chunkOffset, long chunkLength) {
    this.length = length;
    this.isdir = isdir;
    this.blockReplication = (short)blockReplication;
    this.blocksize = blocksize;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    if (permission != null) {
      this.permission = permission;
    } else {
      this.permission = isdir
        ? FsPermission.getDirDefault()
        : FsPermission.getFileDefault();
    }
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.path = path;
    this.chunkOffset = chunkOffset;
    this.chunkLength = chunkLength;
  }

  public CopyListingFileStatus(CopyListingFileStatus other) {
    this.length = other.length;
    this.isdir = other.isdir;
    this.blockReplication = other.blockReplication;
    this.blocksize = other.blocksize;
    this.modificationTime = other.modificationTime;
    this.accessTime = other.accessTime;
    this.permission = other.permission;
    this.owner = other.owner;
    this.group = other.group;
    this.path = new Path(other.path.toUri());
    this.chunkOffset = other.chunkOffset;
    this.chunkLength = other.chunkLength;
  }

  public Path getPath() {
    return path;
  }

  public long getLen() {
    return length;
  }

  public long getBlockSize() {
    return blocksize;
  }

  public boolean isDirectory() {
    return isdir;
  }

  public short getReplication() {
    return blockReplication;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public FsPermission getPermission() {
    return permission;
  }

  public boolean isErasureCoded() {
    return getPermission().getErasureCodedBit();
  }

  /**
   * Returns the full logical ACL.
   *
   * @return List containing full logical ACL
   */
  public List<AclEntry> getAclEntries() {
    return AclUtil.getAclFromPermAndEntries(getPermission(),
      aclEntries != null ? aclEntries : Collections.<AclEntry>emptyList());
  }

  /**
   * Sets optional ACL entries.
   *
   * @param aclEntries List containing all ACL entries
   */
  public void setAclEntries(List<AclEntry> aclEntries) {
    this.aclEntries = aclEntries;
  }
  
  /**
   * Returns all xAttrs.
   * 
   * @return Map containing all xAttrs
   */
  public Map<String, byte[]> getXAttrs() {
    return xAttrs != null ? xAttrs : Collections.<String, byte[]>emptyMap();
  }
  
  /**
   * Sets optional xAttrs.
   * 
   * @param xAttrs Map containing all xAttrs
   */
  public void setXAttrs(Map<String, byte[]> xAttrs) {
    this.xAttrs = xAttrs;
  }

  public long getChunkOffset() {
    return chunkOffset;
  }

  public void setChunkOffset(long offset) {
    this.chunkOffset = offset;
  }

  public long getChunkLength() {
    return chunkLength;
  }

  public void setChunkLength(long chunkLength) {
    this.chunkLength = chunkLength;
  }

  public boolean isSplit() {
    return getChunkLength() != Long.MAX_VALUE &&
        getChunkLength() != getLen();
  }

  public long getSizeToCopy() {
    return isSplit()? getChunkLength() : getLen();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString(), Text.DEFAULT_MAX_LEN);
    out.writeLong(getLen());
    out.writeBoolean(isDirectory());
    out.writeShort(getReplication());
    out.writeLong(getBlockSize());
    out.writeLong(getModificationTime());
    out.writeLong(getAccessTime());
    out.writeShort(getPermission().toShort());
    Text.writeString(out, getOwner(), Text.DEFAULT_MAX_LEN);
    Text.writeString(out, getGroup(), Text.DEFAULT_MAX_LEN);
    if (aclEntries != null) {
      // byte is sufficient, because 32 ACL entries is the max enforced by HDFS.
      out.writeByte(aclEntries.size());
      for (AclEntry entry: aclEntries) {
        out.writeByte(entry.getScope().ordinal());
        out.writeByte(entry.getType().ordinal());
        WritableUtils.writeString(out, entry.getName());
        out.writeByte(entry.getPermission().ordinal());
      }
    } else {
      out.writeByte(NO_ACL_ENTRIES);
    }
    
    if (xAttrs != null) {
      out.writeInt(xAttrs.size());
      Iterator<Entry<String, byte[]>> iter = xAttrs.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, byte[]> entry = iter.next();
        WritableUtils.writeString(out, entry.getKey());
        final byte[] value = entry.getValue();
        if (value != null) {
          out.writeInt(value.length);
          if (value.length > 0) {
            out.write(value);
          }
        } else {
          out.writeInt(-1);
        }
      }
    } else {
      out.writeInt(NO_XATTRS);
    }

    out.writeLong(chunkOffset);
    out.writeLong(chunkLength);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String strPath = Text.readString(in, Text.DEFAULT_MAX_LEN);
    this.path = new Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.blockReplication = in.readShort();
    blocksize = in.readLong();
    modificationTime = in.readLong();
    accessTime = in.readLong();
    permission.fromShort(in.readShort());
    owner = Text.readString(in, Text.DEFAULT_MAX_LEN);
    group = Text.readString(in, Text.DEFAULT_MAX_LEN);
    byte aclEntriesSize = in.readByte();
    if (aclEntriesSize != NO_ACL_ENTRIES) {
      aclEntries = Lists.newArrayListWithCapacity(aclEntriesSize);
      for (int i = 0; i < aclEntriesSize; ++i) {
        aclEntries.add(new AclEntry.Builder()
          .setScope(ACL_ENTRY_SCOPES[in.readByte()])
          .setType(ACL_ENTRY_TYPES[in.readByte()])
          .setName(WritableUtils.readString(in))
          .setPermission(FS_ACTIONS[in.readByte()])
          .build());
      }
    } else {
      aclEntries = null;
    }
    
    int xAttrsSize = in.readInt();
    if (xAttrsSize != NO_XATTRS) {
      xAttrs = Maps.newHashMap();
      for (int i = 0; i < xAttrsSize; ++i) {
        final String name = WritableUtils.readString(in);
        final int valueLen = in.readInt();
        byte[] value = null;
        if (valueLen > -1) {
          value = new byte[valueLen];
          if (valueLen > 0) {
            in.readFully(value);
          }
        }
        xAttrs.put(name, value);
      }
    } else {
      xAttrs = null;
    }

    chunkOffset = in.readLong();
    chunkLength = in.readLong();
  }

  @Override
  public boolean equals(Object o) {
    if (null == o) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    CopyListingFileStatus other = (CopyListingFileStatus)o;
    return getPath().equals(other.getPath())
      && Objects.equal(aclEntries, other.aclEntries)
      && Objects.equal(xAttrs, other.xAttrs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), aclEntries, xAttrs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append('{');
    sb.append(this.getPath().toString());
    sb.append(" length = ").append(this.getLen());
    sb.append(" aclEntries = ").append(aclEntries);
    sb.append(", xAttrs = ").append(xAttrs);
    if (isSplit()) {
      sb.append(", chunkOffset = ").append(this.getChunkOffset());
      sb.append(", chunkLength = ").append(this.getChunkLength());
    }
    sb.append('}');
    return sb.toString();
  }
}
