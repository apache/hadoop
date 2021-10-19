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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Class to pack an AclEntry into an integer. <br>
 * An ACL entry is represented by a 32-bit integer in Big Endian format. <br>
 *
 * Note:  this format is used both in-memory and on-disk.  Changes will be
 * incompatible.
 *
 */
public enum AclEntryStatusFormat implements LongBitFormat.Enum {

  PERMISSION(null, 3),
  TYPE(PERMISSION.BITS, 2),
  SCOPE(TYPE.BITS, 1),
  NAME(SCOPE.BITS, 24);

  private static final FsAction[] FSACTION_VALUES = FsAction.values();
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
      AclEntryScope.values();
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
      AclEntryType.values();

  private final LongBitFormat BITS;

  private AclEntryStatusFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  static AclEntryScope getScope(int aclEntry) {
    int ordinal = (int) SCOPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_SCOPE_VALUES[ordinal];
  }

  static AclEntryType getType(int aclEntry) {
    int ordinal = (int) TYPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_TYPE_VALUES[ordinal];
  }

  static FsAction getPermission(int aclEntry) {
    int ordinal = (int) PERMISSION.BITS.retrieve(aclEntry);
    return FSACTION_VALUES[ordinal];
  }

  static String getName(int aclEntry) {
    return getName(aclEntry, null);
  }

  static String getName(int aclEntry,
                        SerialNumberManager.StringTable stringTable) {
    SerialNumberManager snm = getSerialNumberManager(getType(aclEntry));
    if (snm != null) {
      int nid = (int)NAME.BITS.retrieve(aclEntry);
      return snm.getString(nid, stringTable);
    }
    return null;
  }

  static int toInt(AclEntry aclEntry) {
    long aclEntryInt = 0;
    aclEntryInt = SCOPE.BITS
        .combine(aclEntry.getScope().ordinal(), aclEntryInt);
    aclEntryInt = TYPE.BITS.combine(aclEntry.getType().ordinal(), aclEntryInt);
    aclEntryInt = PERMISSION.BITS.combine(aclEntry.getPermission().ordinal(),
        aclEntryInt);
    SerialNumberManager snm = getSerialNumberManager(aclEntry.getType());
    if (snm != null) {
      int nid = snm.getSerialNumber(aclEntry.getName());
      aclEntryInt = NAME.BITS.combine(nid, aclEntryInt);
    }
    return (int) aclEntryInt;
  }

  static AclEntry toAclEntry(int aclEntry) {
    return toAclEntry(aclEntry, null);
  }

  static AclEntry toAclEntry(int aclEntry,
                             SerialNumberManager.StringTable stringTable) {
    return new AclEntry.Builder()
        .setScope(getScope(aclEntry))
        .setType(getType(aclEntry))
        .setPermission(getPermission(aclEntry))
        .setName(getName(aclEntry, stringTable))
        .build();
  }

  public static int[] toInt(List<AclEntry> aclEntries) {
    int[] entries = new int[aclEntries.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = toInt(aclEntries.get(i));
    }
    return entries;
  }

  private static SerialNumberManager getSerialNumberManager(AclEntryType type) {
    switch (type) {
      case USER:
        return SerialNumberManager.USER;
      case GROUP:
        return SerialNumberManager.GROUP;
      default:
        return null;
    }
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }
}