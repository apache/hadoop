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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * CopyListingFileStatus is a specialized subclass of {@link FileStatus} for
 * attaching additional data members useful to distcp.  This class does not
 * override {@link FileStatus#compareTo}, because the additional data members
 * are not relevant to sort order.
 */
@InterfaceAudience.Private
public final class CopyListingFileStatus extends FileStatus {

  private static final byte NO_ACL_ENTRIES = -1;

  // Retain static arrays of enum values to prevent repeated allocation of new
  // arrays during deserialization.
  private static final AclEntryType[] ACL_ENTRY_TYPES = AclEntryType.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPES = AclEntryScope.values();
  private static final FsAction[] FS_ACTIONS = FsAction.values();

  private List<AclEntry> aclEntries;

  /**
   * Default constructor.
   */
  public CopyListingFileStatus() {
  }

  /**
   * Creates a new CopyListingFileStatus by copying the members of the given
   * FileStatus.
   *
   * @param fileStatus FileStatus to copy
   */
  public CopyListingFileStatus(FileStatus fileStatus) throws IOException {
    super(fileStatus);
  }

  /**
   * Returns the full logical ACL.
   *
   * @return List<AclEntry> containing full logical ACL
   */
  public List<AclEntry> getAclEntries() {
    return AclUtil.getAclFromPermAndEntries(getPermission(),
      aclEntries != null ? aclEntries : Collections.<AclEntry>emptyList());
  }

  /**
   * Sets optional ACL entries.
   *
   * @param aclEntries List<AclEntry> containing all ACL entries
   */
  public void setAclEntries(List<AclEntry> aclEntries) {
    this.aclEntries = aclEntries;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
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
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
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
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    CopyListingFileStatus other = (CopyListingFileStatus)o;
    return Objects.equal(aclEntries, other.aclEntries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), aclEntries);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append('{');
    sb.append("aclEntries = " + aclEntries);
    sb.append('}');
    return sb.toString();
  }
}
