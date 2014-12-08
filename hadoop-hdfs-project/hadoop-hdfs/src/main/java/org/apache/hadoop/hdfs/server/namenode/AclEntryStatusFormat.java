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

import com.google.common.collect.ImmutableList;

/**
 * Class to pack an AclEntry into an integer. <br>
 * An ACL entry is represented by a 32-bit integer in Big Endian format. <br>
 * The bits can be divided in four segments: <br>
 * [0:1) || [1:3) || [3:6) || [6:7) || [7:32) <br>
 * <br>
 * [0:1) -- the scope of the entry (AclEntryScope) <br>
 * [1:3) -- the type of the entry (AclEntryType) <br>
 * [3:6) -- the permission of the entry (FsAction) <br>
 * [6:7) -- A flag to indicate whether Named entry or not <br>
 * [7:32) -- the name of the entry, which is an ID that points to a <br>
 * string in the StringTableSection. <br>
 */
public enum AclEntryStatusFormat {

  SCOPE(null, 1),
  TYPE(SCOPE.BITS, 2),
  PERMISSION(TYPE.BITS, 3),
  NAMED_ENTRY_CHECK(PERMISSION.BITS, 1),
  NAME(NAMED_ENTRY_CHECK.BITS, 25);

  private final LongBitFormat BITS;

  private AclEntryStatusFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  static AclEntryScope getScope(int aclEntry) {
    int ordinal = (int) SCOPE.BITS.retrieve(aclEntry);
    return AclEntryScope.values()[ordinal];
  }

  static AclEntryType getType(int aclEntry) {
    int ordinal = (int) TYPE.BITS.retrieve(aclEntry);
    return AclEntryType.values()[ordinal];
  }

  static FsAction getPermission(int aclEntry) {
    int ordinal = (int) PERMISSION.BITS.retrieve(aclEntry);
    return FsAction.values()[ordinal];
  }

  static String getName(int aclEntry) {
    int nameExists = (int) NAMED_ENTRY_CHECK.BITS.retrieve(aclEntry);
    if (nameExists == 0) {
      return null;
    }
    int id = (int) NAME.BITS.retrieve(aclEntry);
    AclEntryType type = getType(aclEntry);
    if (type == AclEntryType.USER) {
      return SerialNumberManager.INSTANCE.getUser(id);
    } else if (type == AclEntryType.GROUP) {
      return SerialNumberManager.INSTANCE.getGroup(id);
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
    if (aclEntry.getName() != null) {
      aclEntryInt = NAMED_ENTRY_CHECK.BITS.combine(1, aclEntryInt);
      if (aclEntry.getType() == AclEntryType.USER) {
        int userId = SerialNumberManager.INSTANCE.getUserSerialNumber(aclEntry
            .getName());
        aclEntryInt = NAME.BITS.combine(userId, aclEntryInt);
      } else if (aclEntry.getType() == AclEntryType.GROUP) {
        int groupId = SerialNumberManager.INSTANCE
            .getGroupSerialNumber(aclEntry.getName());
        aclEntryInt = NAME.BITS.combine(groupId, aclEntryInt);
      }
    }
    return (int) aclEntryInt;
  }

  static AclEntry toAclEntry(int aclEntry) {
    AclEntry.Builder builder = new AclEntry.Builder();
    builder.setScope(getScope(aclEntry)).setType(getType(aclEntry))
        .setPermission(getPermission(aclEntry));
    if (getName(aclEntry) != null) {
      builder.setName(getName(aclEntry));
    }
    return builder.build();
  }

  public static int[] toInt(List<AclEntry> aclEntries) {
    int[] entries = new int[aclEntries.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = toInt(aclEntries.get(i));
    }
    return entries;
  }

  public static ImmutableList<AclEntry> toAclEntries(int[] entries) {
    ImmutableList.Builder<AclEntry> b = new ImmutableList.Builder<AclEntry>();
    for (int entry : entries) {
      AclEntry aclEntry = toAclEntry(entry);
      b.add(aclEntry);
    }
    return b.build();
  }
}