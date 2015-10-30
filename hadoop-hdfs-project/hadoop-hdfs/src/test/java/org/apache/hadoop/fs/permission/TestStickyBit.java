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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStickyBit {

  static final UserGroupInformation user1 =
    UserGroupInformation.createUserForTesting("theDoctor", new String[] {"tardis"});
  static final UserGroupInformation user2 =
    UserGroupInformation.createUserForTesting("rose", new String[] {"powellestates"});

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem hdfs;
  private static FileSystem hdfsAsUser1;
  private static FileSystem hdfsAsUser2;

  @BeforeClass
  public static void init() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    initCluster(true);
  }

  private static void initCluster(boolean format) throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).format(format)
      .build();
    hdfs = cluster.getFileSystem();
    assertTrue(hdfs instanceof DistributedFileSystem);
    hdfsAsUser1 = DFSTestUtil.getFileSystemAs(user1, conf);
    assertTrue(hdfsAsUser1 instanceof DistributedFileSystem);
    hdfsAsUser2 = DFSTestUtil.getFileSystemAs(user2, conf);
    assertTrue(hdfsAsUser2 instanceof DistributedFileSystem);
  }

  @Before
  public void setup() throws Exception {
    if (hdfs != null) {
      for (FileStatus stat: hdfs.listStatus(new Path("/"))) {
        hdfs.delete(stat.getPath(), true);
      }
    }
  }

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, hdfs, hdfsAsUser1, hdfsAsUser2);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Ensure that even if a file is in a directory with the sticky bit on,
   * another user can write to that file (assuming correct permissions).
   */
  private void confirmCanAppend(Configuration conf, Path p) throws Exception {
    // Write a file to the new tmp directory as a regular user
    Path file = new Path(p, "foo");
    writeFile(hdfsAsUser1, file);
    hdfsAsUser1.setPermission(file, new FsPermission((short) 0777));

    // Log onto cluster as another user and attempt to append to file
    Path file2 = new Path(p, "foo");
    FSDataOutputStream h = null;
    try {
      h = hdfsAsUser2.append(file2);
      h.write("Some more data".getBytes());
      h.close();
      h = null;
    } finally {
      IOUtils.cleanup(null, h);
    }
  }

  /**
   * Test that one user can't delete another user's file when the sticky bit is
   * set.
   */
  private void confirmDeletingFiles(Configuration conf, Path p)
      throws Exception {
    // Write a file to the new temp directory as a regular user
    Path file = new Path(p, "foo");
    writeFile(hdfsAsUser1, file);

    // Make sure the correct user is the owner
    assertEquals(user1.getShortUserName(),
      hdfsAsUser1.getFileStatus(file).getOwner());

    // Log onto cluster as another user and attempt to delete the file
    try {
      hdfsAsUser2.delete(file, false);
      fail("Shouldn't be able to delete someone else's file with SB on");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof AccessControlException);
      assertTrue(ioe.getMessage().contains("sticky bit"));
      assertTrue(ioe.getMessage().contains("user="+user2.getUserName()));
      assertTrue(ioe.getMessage().contains("path=\"" + file + "\""));
      assertTrue(ioe.getMessage().contains("parent=\"" + file.getParent() + "\""));
    }
  }

  /**
   * Test that if a directory is created in a directory that has the sticky bit
   * on, the new directory does not automatically get a sticky bit, as is
   * standard Unix behavior
   */
  private void confirmStickyBitDoesntPropagate(FileSystem hdfs, Path p)
      throws IOException {
    // Create a subdirectory within it
    Path p2 = new Path(p, "bar");
    hdfs.mkdirs(p2);

    // Ensure new directory doesn't have its sticky bit on
    assertFalse(hdfs.getFileStatus(p2).getPermission().getStickyBit());
  }

  /**
   * Test basic ability to get and set sticky bits on files and directories.
   */
  private void confirmSettingAndGetting(FileSystem hdfs, Path p, Path baseDir)
      throws IOException {
    // Initially sticky bit should not be set
    assertFalse(hdfs.getFileStatus(p).getPermission().getStickyBit());

    // Same permission, but with sticky bit on
    short withSB;
    withSB = (short) (hdfs.getFileStatus(p).getPermission().toShort() | 01000);

    assertTrue((new FsPermission(withSB)).getStickyBit());

    hdfs.setPermission(p, new FsPermission(withSB));
    assertTrue(hdfs.getFileStatus(p).getPermission().getStickyBit());

    // Write a file to the fs, try to set its sticky bit
    Path f = new Path(baseDir, "somefile");
    writeFile(hdfs, f);
    assertFalse(hdfs.getFileStatus(f).getPermission().getStickyBit());

    withSB = (short) (hdfs.getFileStatus(f).getPermission().toShort() | 01000);

    hdfs.setPermission(f, new FsPermission(withSB));

    assertTrue(hdfs.getFileStatus(f).getPermission().getStickyBit());
  }

  @Test
  public void testGeneralSBBehavior() throws Exception {
    Path baseDir = new Path("/mcgann");
    hdfs.mkdirs(baseDir);

    // Create a tmp directory with wide-open permissions and sticky bit
    Path p = new Path(baseDir, "tmp");

    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));

    confirmCanAppend(conf, p);

    baseDir = new Path("/eccleston");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "roguetraders");

    hdfs.mkdirs(p);
    confirmSettingAndGetting(hdfs, p, baseDir);

    baseDir = new Path("/tennant");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "contemporary");
    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));
    confirmDeletingFiles(conf, p);

    baseDir = new Path("/smith");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "scissorsisters");

    // Turn on its sticky bit
    hdfs.mkdirs(p, new FsPermission((short) 01666));
    confirmStickyBitDoesntPropagate(hdfs, baseDir);
  }

  @Test
  public void testAclGeneralSBBehavior() throws Exception {
    Path baseDir = new Path("/mcgann");
    hdfs.mkdirs(baseDir);

    // Create a tmp directory with wide-open permissions and sticky bit
    Path p = new Path(baseDir, "tmp");

    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));
    applyAcl(p);
    confirmCanAppend(conf, p);

    baseDir = new Path("/eccleston");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "roguetraders");

    hdfs.mkdirs(p);
    applyAcl(p);
    confirmSettingAndGetting(hdfs, p, baseDir);

    baseDir = new Path("/tennant");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "contemporary");
    hdfs.mkdirs(p);
    hdfs.setPermission(p, new FsPermission((short) 01777));
    applyAcl(p);
    confirmDeletingFiles(conf, p);

    baseDir = new Path("/smith");
    hdfs.mkdirs(baseDir);
    p = new Path(baseDir, "scissorsisters");

    // Turn on its sticky bit
    hdfs.mkdirs(p, new FsPermission((short) 01666));
    applyAcl(p);
    confirmStickyBitDoesntPropagate(hdfs, p);
  }

  /**
   * Test that one user can't rename/move another user's file when the sticky
   * bit is set.
   */
  @Test
  public void testMovingFiles() throws Exception {
    testMovingFiles(false);
  }

  @Test
  public void testAclMovingFiles() throws Exception {
    testMovingFiles(true);
  }

  private void testMovingFiles(boolean useAcl) throws Exception {
    // Create a tmp directory with wide-open permissions and sticky bit
    Path tmpPath = new Path("/tmp");
    Path tmpPath2 = new Path("/tmp2");
    hdfs.mkdirs(tmpPath);
    hdfs.mkdirs(tmpPath2);
    hdfs.setPermission(tmpPath, new FsPermission((short) 01777));
    if (useAcl) {
      applyAcl(tmpPath);
    }
    hdfs.setPermission(tmpPath2, new FsPermission((short) 01777));
    if (useAcl) {
      applyAcl(tmpPath2);
    }

    // Write a file to the new tmp directory as a regular user
    Path file = new Path(tmpPath, "foo");

    writeFile(hdfsAsUser1, file);

    // Log onto cluster as another user and attempt to move the file
    try {
      hdfsAsUser2.rename(file, new Path(tmpPath2, "renamed"));
      fail("Shouldn't be able to rename someone else's file with SB on");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof AccessControlException);
      assertTrue(ioe.getMessage().contains("sticky bit"));
    }
  }

  /**
   * Ensure that when we set a sticky bit and shut down the file system, we get
   * the sticky bit back on re-start, and that no extra sticky bits appear after
   * re-start.
   */
  @Test
  public void testStickyBitPersistence() throws Exception {
    // A tale of three directories...
    Path sbSet = new Path("/Housemartins");
    Path sbNotSpecified = new Path("/INXS");
    Path sbSetOff = new Path("/Easyworld");

    for (Path p : new Path[] { sbSet, sbNotSpecified, sbSetOff })
      hdfs.mkdirs(p);

    // Two directories had there sticky bits set explicitly...
    hdfs.setPermission(sbSet, new FsPermission((short) 01777));
    hdfs.setPermission(sbSetOff, new FsPermission((short) 00777));

    shutdown();

    // Start file system up again
    initCluster(false);

    assertTrue(hdfs.exists(sbSet));
    assertTrue(hdfs.getFileStatus(sbSet).getPermission().getStickyBit());

    assertTrue(hdfs.exists(sbNotSpecified));
    assertFalse(hdfs.getFileStatus(sbNotSpecified).getPermission()
        .getStickyBit());

    assertTrue(hdfs.exists(sbSetOff));
    assertFalse(hdfs.getFileStatus(sbSetOff).getPermission().getStickyBit());
  }

  @Test
  public void testAclStickyBitPersistence() throws Exception {
    // A tale of three directories...
    Path sbSet = new Path("/Housemartins");
    Path sbNotSpecified = new Path("/INXS");
    Path sbSetOff = new Path("/Easyworld");

    for (Path p : new Path[] { sbSet, sbNotSpecified, sbSetOff })
      hdfs.mkdirs(p);

    // Two directories had there sticky bits set explicitly...
    hdfs.setPermission(sbSet, new FsPermission((short) 01777));
    applyAcl(sbSet);
    hdfs.setPermission(sbSetOff, new FsPermission((short) 00777));
    applyAcl(sbSetOff);

    shutdown();

    // Start file system up again
    initCluster(false);

    assertTrue(hdfs.exists(sbSet));
    assertTrue(hdfs.getFileStatus(sbSet).getPermission().getStickyBit());

    assertTrue(hdfs.exists(sbNotSpecified));
    assertFalse(hdfs.getFileStatus(sbNotSpecified).getPermission()
        .getStickyBit());

    assertTrue(hdfs.exists(sbSetOff));
    assertFalse(hdfs.getFileStatus(sbSetOff).getPermission().getStickyBit());
  }

  /***
   * Write a quick file to the specified file system at specified path
   */
  static private void writeFile(FileSystem hdfs, Path p) throws IOException {
    FSDataOutputStream o = null;
    try {
      o = hdfs.create(p);
      o.write("some file contents".getBytes());
      o.close();
      o = null;
    } finally {
      IOUtils.cleanup(null, o);
    }
  }

  /**
   * Applies an ACL (both access and default) to the given path.
   *
   * @param p Path to set
   * @throws IOException if an ACL could not be modified
   */
  private static void applyAcl(Path p) throws IOException {
    hdfs.modifyAclEntries(p, Arrays.asList(
      aclEntry(ACCESS, USER, user2.getShortUserName(), ALL),
      aclEntry(DEFAULT, USER, user2.getShortUserName(), ALL)));
  }
}
