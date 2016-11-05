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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAclCommands {
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private String path;

  private Configuration conf = null;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    path = testFolder.newFile("file").getPath();
  }

  @Test
  public void testGetfaclValidations() throws Exception {
    assertFalse("getfacl should fail without path",
        0 == runCommand(new String[] {"-getfacl"}));
    assertFalse("getfacl should fail with extra argument",
        0 == runCommand(new String[] {"-getfacl", path, "extraArg"}));
  }

  @Test
  public void testSetfaclValidations() throws Exception {
    assertFalse("setfacl should fail without options",
        0 == runCommand(new String[] {"-setfacl", path}));
    assertFalse("setfacl should fail without options -b, -k, -m, -x or --set",
        0 == runCommand(new String[] {"-setfacl", "-R", path}));
    assertFalse("setfacl should fail without path",
        0 == runCommand(new String[] {"-setfacl"}));
    assertFalse("setfacl should fail without aclSpec",
        0 == runCommand(new String[] {"-setfacl", "-m", path}));
    assertFalse("setfacl should fail with conflicting options",
        0 == runCommand(new String[] {"-setfacl", "-m", path}));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] {"-setfacl", path, "extra"}));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] {"-setfacl", "--set",
            "default:user::rwx", path, "extra"}));
    assertFalse("setfacl should fail with permissions for -x",
        0 == runCommand(new String[] {"-setfacl", "-x", "user:user1:rwx",
            path}));
    assertFalse("setfacl should fail ACL spec missing",
        0 == runCommand(new String[] {"-setfacl", "-m", "", path}));
  }

  @Test
  public void testSetfaclValidationsWithoutPermissions() throws Exception {
    List<AclEntry> parsedList = new ArrayList<AclEntry>();
    try {
      parsedList = AclEntry.parseAclSpec("user:user1:", true);
    } catch (IllegalArgumentException e) {
    }
    assertTrue(parsedList.size() == 0);
    assertFalse("setfacl should fail with less arguments",
        0 == runCommand(new String[] { "-setfacl", "-m", "user:user1:",
            "/path" }));
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

  @Test
  public void testLsNoRpcForGetAclStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///");
    conf.setClass("fs.stubfs.impl", StubFileSystem.class, FileSystem.class);
    conf.setBoolean("stubfs.noRpcForGetAclStatus", true);
    assertEquals("ls must succeed even if getAclStatus RPC does not exist.",
      0, ToolRunner.run(conf, new FsShell(), new String[] { "-ls", "/" }));
  }

  @Test
  public void testLsAclsUnsupported() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///");
    conf.setClass("fs.stubfs.impl", StubFileSystem.class, FileSystem.class);
    assertEquals("ls must succeed even if FileSystem does not implement ACLs.",
      0, ToolRunner.run(conf, new FsShell(), new String[] { "-ls", "/" }));
  }

  public static class StubFileSystem extends FileSystem {

    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    public AclStatus getAclStatus(Path path) throws IOException {
      if (getConf().getBoolean("stubfs.noRpcForGetAclStatus", false)) {
        throw new RemoteException(RpcNoSuchMethodException.class.getName(),
          "test exception");
      }
      return super.getAclStatus(path);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (f.isRoot()) {
        return new FileStatus(0, true, 0, 0, 0, f);
      }
      return null;
    }

    @Override
    public URI getUri() {
      return URI.create("stubfs:///");
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      FsPermission perm = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE,
        FsAction.READ_EXECUTE);
      Path path = new Path("/foo");
      FileStatus stat = new FileStatus(1000, true, 3, 1000, 0, 0, perm, "owner",
        "group", path);
      return new FileStatus[] { stat };
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
        throws IOException {
      return false;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
    }
  }

  private int runCommand(String[] commands) throws Exception {
    return ToolRunner.run(conf, new FsShell(), commands);
  }
}
