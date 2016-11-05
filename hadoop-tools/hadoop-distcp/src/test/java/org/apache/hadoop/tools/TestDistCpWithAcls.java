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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests distcp in combination with HDFS ACLs.
 */
public class TestDistCpWithAcls {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;

  @BeforeClass
  public static void init() throws Exception {
    initCluster(true, true);
    // Create this directory structure:
    // /src
    //   /dir1
    //     /subdir1
    //   /dir2
    //     /dir2/file2
    //     /dir2/file3
    //   /dir3sticky
    //   /file1    
    fs.mkdirs(new Path("/src/dir1/subdir1"));
    fs.mkdirs(new Path("/src/dir2"));
    fs.create(new Path("/src/dir2/file2")).close();
    fs.create(new Path("/src/dir2/file3")).close();
    fs.mkdirs(new Path("/src/dir3sticky"));
    fs.create(new Path("/src/file1")).close();

    // Set a mix of ACLs and plain permissions throughout the tree.
    fs.modifyAclEntries(new Path("/src/dir1"), Arrays.asList(
      aclEntry(DEFAULT, USER, "bruce", ALL)));

    fs.modifyAclEntries(new Path("/src/dir2/file2"), Arrays.asList(
      aclEntry(ACCESS, GROUP, "sales", NONE)));

    fs.setPermission(new Path("/src/dir2/file3"),
      new FsPermission((short)0660));

    fs.modifyAclEntries(new Path("/src/file1"), Arrays.asList(
      aclEntry(ACCESS, USER, "diana", READ)));

    fs.setPermission(new Path("/src/dir3sticky"),
      new FsPermission((short)01777));
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPreserveAcls() throws Exception {
    assertRunDistCp(DistCpConstants.SUCCESS, "/dstPreserveAcls");

    assertAclEntries("/dstPreserveAcls/dir1", new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE) } );
    assertPermission("/dstPreserveAcls/dir1", (short)0755);

    assertAclEntries("/dstPreserveAcls/dir1/subdir1", new AclEntry[] { });
    assertPermission("/dstPreserveAcls/dir1/subdir1", (short)0755);

    assertAclEntries("/dstPreserveAcls/dir2", new AclEntry[] { });
    assertPermission("/dstPreserveAcls/dir2", (short)0755);

    assertAclEntries("/dstPreserveAcls/dir2/file2", new AclEntry[] {
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "sales", NONE) } );
    assertPermission("/dstPreserveAcls/dir2/file2", (short)0644);

    assertAclEntries("/dstPreserveAcls/dir2/file3", new AclEntry[] { });
    assertPermission("/dstPreserveAcls/dir2/file3", (short)0660);

    assertAclEntries("/dstPreserveAcls/dir3sticky", new AclEntry[] { });
    assertPermission("/dstPreserveAcls/dir3sticky", (short)01777);

    assertAclEntries("/dstPreserveAcls/file1", new AclEntry[] {
      aclEntry(ACCESS, USER, "diana", READ),
      aclEntry(ACCESS, GROUP, READ) } );
    assertPermission("/dstPreserveAcls/file1", (short)0644);
  }

  @Test
  public void testAclsNotEnabled() throws Exception {
    try {
      restart(false);
      assertRunDistCp(DistCpConstants.ACLS_NOT_SUPPORTED, "/dstAclsNotEnabled");
    } finally {
      restart(true);
    }
  }

  @Test
  public void testAclsNotImplemented() throws Exception {
    assertRunDistCp(DistCpConstants.ACLS_NOT_SUPPORTED,
      "stubfs://dstAclsNotImplemented");
  }

  /**
   * Stub FileSystem implementation used for testing the case of attempting
   * distcp with ACLs preserved on a file system that does not support ACLs.
   * The base class implementation throws UnsupportedOperationException for the
   * ACL methods, so we don't need to override them.
   */
  public static class StubFileSystem extends FileSystem {

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public URI getUri() {
      return URI.create("stubfs:///");
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path(Path.SEPARATOR);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return new FileStatus[0];
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

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param permission FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setPermission(permission)
      .build();
  }

  /**
   * Create a new AclEntry with scope, type, name and permission.
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param name String optional ACL entry name
   * @param permission FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name, FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setName(name)
      .setPermission(permission)
      .build();
  }

  /**
   * Asserts the ACL entries returned by getAclStatus for a specific path.
   *
   * @param path String path to check
   * @param entries AclEntry[] expected ACL entries
   * @throws Exception if there is any error
   */
  private static void assertAclEntries(String path, AclEntry[] entries)
      throws Exception {
    assertArrayEquals(entries, fs.getAclStatus(new Path(path)).getEntries()
      .toArray(new AclEntry[0]));
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of a specific path.
   *
   * @param path String path to check
   * @param perm short expected permission bits
   * @throws Exception if there is any error
   */
  private static void assertPermission(String path, short perm)
      throws Exception {
    assertEquals(perm,
      fs.getFileStatus(new Path(path)).getPermission().toShort());
  }

  /**
   * Runs distcp from /src to specified destination, preserving ACLs.  Asserts
   * expected exit code.
   *
   * @param int exitCode expected exit code
   * @param dst String distcp destination
   * @throws Exception if there is any error
   */
  private static void assertRunDistCp(int exitCode, String dst)
      throws Exception {
    DistCp distCp = new DistCp(conf, null);
    assertEquals(exitCode, ToolRunner.run(
      conf, distCp, new String[] { "-pa", "/src", dst }));
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param aclsEnabled if true, ACL support is enabled
   * @throws Exception if any step fails
   */
  private static void initCluster(boolean format, boolean aclsEnabled)
      throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, aclsEnabled);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///");
    conf.setClass("fs.stubfs.impl", StubFileSystem.class, FileSystem.class);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  /**
   * Restarts the cluster with ACLs enabled or disabled.
   *
   * @param aclsEnabled if true, ACL support is enabled
   * @throws Exception if any step fails
   */
  private static void restart(boolean aclsEnabled) throws Exception {
    shutdown();
    initCluster(false, aclsEnabled);
  }
}
