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
package org.apache.hadoop.fs.permission;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests covering basic functionality of the ACL objects.
 */
public class TestAcl {
  private static AclEntry ENTRY1, ENTRY2, ENTRY3, ENTRY4, ENTRY5, ENTRY6,
    ENTRY7, ENTRY8, ENTRY9, ENTRY10, ENTRY11, ENTRY12, ENTRY13;
  private static AclStatus STATUS1, STATUS2, STATUS3, STATUS4;

  @BeforeClass
  public static void setUp() {
    // named user
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setName("user1")
      .setPermission(FsAction.ALL);
    ENTRY1 = aclEntryBuilder.build();
    ENTRY2 = aclEntryBuilder.build();
    // named group
    ENTRY3 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setName("group2")
      .setPermission(FsAction.READ_WRITE)
      .build();
    // default other
    ENTRY4 = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setPermission(FsAction.NONE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // owner
    ENTRY5 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setPermission(FsAction.ALL)
      .build();
    // default named group
    ENTRY6 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setName("group3")
      .setPermission(FsAction.READ_WRITE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // other
    ENTRY7 = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setPermission(FsAction.NONE)
      .build();
    // default named user
    ENTRY8 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setName("user3")
      .setPermission(FsAction.ALL)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // mask
    ENTRY9 = new AclEntry.Builder()
      .setType(AclEntryType.MASK)
      .setPermission(FsAction.READ)
      .build();
    // default mask
    ENTRY10 = new AclEntry.Builder()
      .setType(AclEntryType.MASK)
      .setPermission(FsAction.READ_EXECUTE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // group
    ENTRY11 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setPermission(FsAction.READ)
      .build();
    // default group
    ENTRY12 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setPermission(FsAction.READ)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // default owner
    ENTRY13 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setPermission(FsAction.ALL)
      .setScope(AclEntryScope.DEFAULT)
      .build();

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder()
      .owner("owner1")
      .group("group1")
      .addEntry(ENTRY1)
      .addEntry(ENTRY3)
      .addEntry(ENTRY4);
    STATUS1 = aclStatusBuilder.build();
    STATUS2 = aclStatusBuilder.build();
    STATUS3 = new AclStatus.Builder()
      .owner("owner2")
      .group("group2")
      .stickyBit(true)
      .build();

    STATUS4 = new AclStatus.Builder()
      .addEntry(ENTRY1)
      .addEntry(ENTRY3)
      .addEntry(ENTRY4)
      .addEntry(ENTRY5)
      .addEntry(ENTRY6)
      .addEntry(ENTRY7)
      .addEntry(ENTRY8)
      .addEntry(ENTRY9)
      .addEntry(ENTRY10)
      .addEntry(ENTRY11)
      .addEntry(ENTRY12)
      .addEntry(ENTRY13)
      .build();
  }

  @Test
  public void testEntryEquals() {
    Assertions.assertThat(ENTRY2).isNotSameAs(ENTRY1);
    Assertions.assertThat(ENTRY3).isNotSameAs(ENTRY1);
    Assertions.assertThat(ENTRY4).isNotSameAs(ENTRY1);
    Assertions.assertThat(ENTRY3).isNotSameAs(ENTRY2);
    Assertions.assertThat(ENTRY4).isNotSameAs(ENTRY2);
    Assertions.assertThat(ENTRY4).isNotSameAs(ENTRY3);
    Assertions.assertThat(ENTRY1).isEqualTo(ENTRY1);
    Assertions.assertThat(ENTRY2).isEqualTo(ENTRY2);
    Assertions.assertThat(ENTRY2).isEqualTo(ENTRY1);
    Assertions.assertThat(ENTRY1).isEqualTo(ENTRY2);
    Assertions.assertThat(ENTRY1).isNotEqualTo(ENTRY3);
    Assertions.assertThat(ENTRY1).isNotEqualTo(ENTRY4);
    Assertions.assertThat(ENTRY3).isNotEqualTo(ENTRY4);
    Assertions.assertThat(ENTRY1).isNotEqualTo(null);
    Assertions.assertThat(ENTRY1).isNotEqualTo(new Object());
  }

  @Test
  public void testEntryHashCode() {
    Assertions.assertThat(ENTRY2.hashCode()).isEqualTo(ENTRY1.hashCode());
    Assertions.assertThat(ENTRY1.hashCode()).isNotEqualTo(ENTRY3.hashCode());
    Assertions.assertThat(ENTRY1.hashCode()).isNotEqualTo(ENTRY4.hashCode());
    Assertions.assertThat(ENTRY3.hashCode()).isNotEqualTo(ENTRY4.hashCode());
  }

  @Test
  public void testEntryScopeIsAccessIfUnspecified() {
    Assertions.assertThat(ENTRY1.getScope()).isEqualTo(AclEntryScope.ACCESS);
    Assertions.assertThat(ENTRY2.getScope()).isEqualTo(AclEntryScope.ACCESS);
    Assertions.assertThat(ENTRY3.getScope()).isEqualTo(AclEntryScope.ACCESS);
    Assertions.assertThat(ENTRY4.getScope()).isEqualTo(AclEntryScope.DEFAULT);
  }

  @Test
  public void testStatusEquals() {
    Assertions.assertThat(STATUS2).isNotSameAs(STATUS1);
    Assertions.assertThat(STATUS3).isNotSameAs(STATUS1);
    Assertions.assertThat(STATUS3).isNotSameAs(STATUS2);
    Assertions.assertThat(STATUS1).isEqualTo(STATUS1);
    Assertions.assertThat(STATUS2).isEqualTo(STATUS2);
    Assertions.assertThat(STATUS2).isEqualTo(STATUS1);
    Assertions.assertThat(STATUS1).isEqualTo(STATUS2);
    Assertions.assertThat(STATUS1).isNotEqualTo(STATUS3);
    Assertions.assertThat(STATUS2).isNotEqualTo(STATUS3);
    Assertions.assertThat(STATUS1).isNotEqualTo(null);
    Assertions.assertThat(STATUS1).isNotEqualTo(new Object());
  }

  @Test
  public void testStatusHashCode() {
    Assertions.assertThat(STATUS2.hashCode()).isEqualTo(STATUS1.hashCode());
    Assertions.assertThat(STATUS1.hashCode()).isNotEqualTo(STATUS3.hashCode());
  }

  @Test
  public void testToString() {
    Assertions.assertThat(ENTRY1.toString()).isEqualTo("user:user1:rwx");
    Assertions.assertThat(ENTRY2.toString()).isEqualTo("user:user1:rwx");
    Assertions.assertThat(ENTRY3.toString()).isEqualTo("group:group2:rw-");
    Assertions.assertThat(ENTRY4.toString()).isEqualTo("default:other::---");

    Assertions.assertThat(STATUS1.toString()).isEqualTo(
        "owner: owner1, group: group1, acl: {entries: "
            + "[user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}");
    Assertions.assertThat(STATUS2.toString()).isEqualTo(
        "owner: owner1, group: group1, acl: {entries: "
            + "[user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}");
    Assertions.assertThat(STATUS3.toString())
        .isEqualTo("owner: owner2, group: group2, acl: {entries: [], stickyBit: true}");
  }
}
