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
package org.apache.hadoop.fs.shell;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

public class TestAclCommands {

  private Configuration conf = null;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
  }

  @Test
  public void testGetfaclValidations() throws Exception {
    assertFalse("getfacl should fail without path",
        0 == runCommand(new String[] { "-getfacl" }));
    assertFalse("getfacl should fail with extra argument",
        0 == runCommand(new String[] { "-getfacl", "/test", "extraArg" }));
  }

  @Test
  public void testSetfaclValidations() throws Exception {
    assertFalse("setfacl should fail without path",
        0 == runCommand(new String[] { "-setfacl" }));
    assertFalse("setfacl should fail without aclSpec",
        0 == runCommand(new String[] { "-setfacl", "-m", "/path" }));
    assertFalse("setfacl should fail with conflicting options",
        0 == runCommand(new String[] { "-setfacl", "-m", "/path" }));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] { "-setfacl", "/path", "extra" }));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] { "-setfacl", "--set",
            "default:user::rwx", "/path", "extra" }));
    assertFalse("setfacl should fail with permissions for -x",
        0 == runCommand(new String[] { "-setfacl", "-x", "user:user1:rwx",
            "/path" }));
    assertFalse("setfacl should fail ACL spec missing",
        0 == runCommand(new String[] { "-setfacl", "-m",
            "", "/path" }));
  }

  @Test
  public void testMultipleAclSpecParsing() throws Exception {
    List<AclEntry> parsedList = AclEntry.parseAclSpec(
        "group::rwx,user:user1:rwx,user:user2:rw-,"
            + "group:group1:rw-,default:group:group1:rw-", true);

    AclEntry basicAcl = new AclEntry.Builder().setType(AclEntryType.GROUP)
        .setPermission(FsAction.ALL).build();
    AclEntry user1Acl = new AclEntry.Builder().setType(AclEntryType.USER)
        .setPermission(FsAction.ALL).setName("user1").build();
    AclEntry user2Acl = new AclEntry.Builder().setType(AclEntryType.USER)
        .setPermission(FsAction.READ_WRITE).setName("user2").build();
    AclEntry group1Acl = new AclEntry.Builder().setType(AclEntryType.GROUP)
        .setPermission(FsAction.READ_WRITE).setName("group1").build();
    AclEntry defaultAcl = new AclEntry.Builder().setType(AclEntryType.GROUP)
        .setPermission(FsAction.READ_WRITE).setName("group1")
        .setScope(AclEntryScope.DEFAULT).build();
    List<AclEntry> expectedList = new ArrayList<AclEntry>();
    expectedList.add(basicAcl);
    expectedList.add(user1Acl);
    expectedList.add(user2Acl);
    expectedList.add(group1Acl);
    expectedList.add(defaultAcl);
    assertEquals("Parsed Acl not correct", expectedList, parsedList);
  }

  @Test
  public void testMultipleAclSpecParsingWithoutPermissions() throws Exception {
    List<AclEntry> parsedList = AclEntry.parseAclSpec(
        "user::,user:user1:,group::,group:group1:,mask::,other::,"
            + "default:user:user1::,default:mask::", false);

    AclEntry owner = new AclEntry.Builder().setType(AclEntryType.USER).build();
    AclEntry namedUser = new AclEntry.Builder().setType(AclEntryType.USER)
        .setName("user1").build();
    AclEntry group = new AclEntry.Builder().setType(AclEntryType.GROUP).build();
    AclEntry namedGroup = new AclEntry.Builder().setType(AclEntryType.GROUP)
        .setName("group1").build();
    AclEntry mask = new AclEntry.Builder().setType(AclEntryType.MASK).build();
    AclEntry other = new AclEntry.Builder().setType(AclEntryType.OTHER).build();
    AclEntry defaultUser = new AclEntry.Builder()
        .setScope(AclEntryScope.DEFAULT).setType(AclEntryType.USER)
        .setName("user1").build();
    AclEntry defaultMask = new AclEntry.Builder()
        .setScope(AclEntryScope.DEFAULT).setType(AclEntryType.MASK).build();
    List<AclEntry> expectedList = new ArrayList<AclEntry>();
    expectedList.add(owner);
    expectedList.add(namedUser);
    expectedList.add(group);
    expectedList.add(namedGroup);
    expectedList.add(mask);
    expectedList.add(other);
    expectedList.add(defaultUser);
    expectedList.add(defaultMask);
    assertEquals("Parsed Acl not correct", expectedList, parsedList);
  }

  private int runCommand(String[] commands) throws Exception {
    return ToolRunner.run(conf, new FsShell(), commands);
  }
}
