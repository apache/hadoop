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

import static org.junit.Assert.*;

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
    assertNotSame(ENTRY1, ENTRY2);
    assertNotSame(ENTRY1, ENTRY3);
    assertNotSame(ENTRY1, ENTRY4);
    assertNotSame(ENTRY2, ENTRY3);
    assertNotSame(ENTRY2, ENTRY4);
    assertNotSame(ENTRY3, ENTRY4);
    assertEquals(ENTRY1, ENTRY1);
    assertEquals(ENTRY2, ENTRY2);
    assertEquals(ENTRY1, ENTRY2);
    assertEquals(ENTRY2, ENTRY1);
    assertFalse(ENTRY1.equals(ENTRY3));
    assertFalse(ENTRY1.equals(ENTRY4));
    assertFalse(ENTRY3.equals(ENTRY4));
    assertFalse(ENTRY1.equals(null));
    assertFalse(ENTRY1.equals(new Object()));
  }

  @Test
  public void testEntryHashCode() {
    assertEquals(ENTRY1.hashCode(), ENTRY2.hashCode());
    assertFalse(ENTRY1.hashCode() == ENTRY3.hashCode());
    assertFalse(ENTRY1.hashCode() == ENTRY4.hashCode());
    assertFalse(ENTRY3.hashCode() == ENTRY4.hashCode());
  }

  @Test
  public void testEntryScopeIsAccessIfUnspecified() {
    assertEquals(AclEntryScope.ACCESS, ENTRY1.getScope());
    assertEquals(AclEntryScope.ACCESS, ENTRY2.getScope());
    assertEquals(AclEntryScope.ACCESS, ENTRY3.getScope());
    assertEquals(AclEntryScope.DEFAULT, ENTRY4.getScope());
  }

  @Test
  public void testStatusEquals() {
    assertNotSame(STATUS1, STATUS2);
    assertNotSame(STATUS1, STATUS3);
    assertNotSame(STATUS2, STATUS3);
    assertEquals(STATUS1, STATUS1);
    assertEquals(STATUS2, STATUS2);
    assertEquals(STATUS1, STATUS2);
    assertEquals(STATUS2, STATUS1);
    assertFalse(STATUS1.equals(STATUS3));
    assertFalse(STATUS2.equals(STATUS3));
    assertFalse(STATUS1.equals(null));
    assertFalse(STATUS1.equals(new Object()));
  }

  @Test
  public void testStatusHashCode() {
    assertEquals(STATUS1.hashCode(), STATUS2.hashCode());
    assertFalse(STATUS1.hashCode() == STATUS3.hashCode());
  }

  @Test
  public void testToString() {
    assertEquals("user:user1:rwx", ENTRY1.toString());
    assertEquals("user:user1:rwx", ENTRY2.toString());
    assertEquals("group:group2:rw-", ENTRY3.toString());
    assertEquals("default:other::---", ENTRY4.toString());

    assertEquals(
      "owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}",
      STATUS1.toString());
    assertEquals(
      "owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}",
      STATUS2.toString());
    assertEquals(
      "owner: owner2, group: group2, acl: {entries: [], stickyBit: true}",
      STATUS3.toString());
  }
}
