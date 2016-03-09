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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;

public class TestErasureCodingPolicies {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private FSNamesystem namesystem;

  @Before
  public void setupCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    namesystem = cluster.getNamesystem();
  }

  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * for pre-existing files (with replicated blocks) in an EC dir, getListing
   * should report them as non-ec.
   */
  @Test(timeout=60000)
  public void testReplicatedFileUnderECDir() throws IOException {
    final Path dir = new Path("/ec");
    final Path replicatedFile = new Path(dir, "replicatedFile");
    // create a file with replicated blocks
    DFSTestUtil.createFile(fs, replicatedFile, 0, (short) 3, 0L);

    // set ec policy on dir
    fs.setErasureCodingPolicy(dir, null);
    // create a file which should be using ec
    final Path ecSubDir = new Path(dir, "ecSubDir");
    final Path ecFile = new Path(ecSubDir, "ecFile");
    DFSTestUtil.createFile(fs, ecFile, 0, (short) 1, 0L);

    assertNull(fs.getClient().getFileInfo(replicatedFile.toString())
        .getErasureCodingPolicy());
    assertNotNull(fs.getClient().getFileInfo(ecFile.toString())
        .getErasureCodingPolicy());

    // list "/ec"
    DirectoryListing listing = fs.getClient().listPaths(dir.toString(),
        new byte[0], false);
    HdfsFileStatus[] files = listing.getPartialListing();
    assertEquals(2, files.length);
    // the listing is always sorted according to the local name
    assertEquals(ecSubDir.getName(), files[0].getLocalName());
    assertNotNull(files[0].getErasureCodingPolicy()); // ecSubDir
    assertEquals(replicatedFile.getName(), files[1].getLocalName());
    assertNull(files[1].getErasureCodingPolicy()); // replicatedFile

    // list "/ec/ecSubDir"
    files = fs.getClient().listPaths(ecSubDir.toString(),
        new byte[0], false).getPartialListing();
    assertEquals(1, files.length);
    assertEquals(ecFile.getName(), files[0].getLocalName());
    assertNotNull(files[0].getErasureCodingPolicy()); // ecFile

    // list "/"
    files = fs.getClient().listPaths("/", new byte[0], false).getPartialListing();
    assertEquals(1, files.length);
    assertEquals(dir.getName(), files[0].getLocalName()); // ec
    assertNotNull(files[0].getErasureCodingPolicy());

    // rename "/ec/ecSubDir/ecFile" to "/ecFile"
    assertTrue(fs.rename(ecFile, new Path("/ecFile")));
    files = fs.getClient().listPaths("/", new byte[0], false).getPartialListing();
    assertEquals(2, files.length);
    assertEquals(dir.getName(), files[0].getLocalName()); // ec
    assertNotNull(files[0].getErasureCodingPolicy());
    assertEquals(ecFile.getName(), files[1].getLocalName());
    assertNotNull(files[1].getErasureCodingPolicy());
  }

  @Test(timeout = 60000)
  public void testBasicSetECPolicy()
      throws IOException, InterruptedException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());

    /* Normal creation of an erasure coding directory */
    fs.getClient().setErasureCodingPolicy(testDir.toString(), null);

    /* Verify files under the directory are striped */
    final Path ECFilePath = new Path(testDir, "foo");
    fs.create(ECFilePath);
    INode inode = namesystem.getFSDirectory().getINode(ECFilePath.toString());
    assertTrue(inode.asFile().isStriped());

    /**
     * Verify that setting EC policy on non-empty directory only affects
     * newly created files under the directory.
     */
    final Path notEmpty = new Path("/nonEmpty");
    fs.mkdir(notEmpty, FsPermission.getDirDefault());
    final Path oldFile = new Path(notEmpty, "old");
    fs.create(oldFile);
    fs.getClient().setErasureCodingPolicy(notEmpty.toString(), null);
    final Path newFile = new Path(notEmpty, "new");
    fs.create(newFile);
    INode oldInode = namesystem.getFSDirectory().getINode(oldFile.toString());
    assertFalse(oldInode.asFile().isStriped());
    INode newInode = namesystem.getFSDirectory().getINode(newFile.toString());
    assertTrue(newInode.asFile().isStriped());

    /* Verify that nested EC policies not supported */
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path(dir1, "dir2");
    fs.mkdir(dir1, FsPermission.getDirDefault());
    fs.getClient().setErasureCodingPolicy(dir1.toString(), null);
    fs.mkdir(dir2, FsPermission.getDirDefault());
    try {
      fs.getClient().setErasureCodingPolicy(dir2.toString(), null);
      fail("Nested erasure coding policies");
    } catch (IOException e) {
      assertExceptionContains("already has an erasure coding policy", e);
    }

    /* Verify that EC policy cannot be set on a file */
    final Path fPath = new Path("/file");
    fs.create(fPath);
    try {
      fs.getClient().setErasureCodingPolicy(fPath.toString(), null);
      fail("Erasure coding policy on file");
    } catch (IOException e) {
      assertExceptionContains("erasure coding policy for a file", e);
    }
  }

  @Test(timeout = 60000)
  public void testMoveValidity() throws IOException, InterruptedException {
    final Path srcECDir = new Path("/srcEC");
    final Path dstECDir = new Path("/dstEC");
    fs.mkdir(srcECDir, FsPermission.getDirDefault());
    fs.mkdir(dstECDir, FsPermission.getDirDefault());
    fs.getClient().setErasureCodingPolicy(srcECDir.toString(), null);
    fs.getClient().setErasureCodingPolicy(dstECDir.toString(), null);
    final Path srcFile = new Path(srcECDir, "foo");
    fs.create(srcFile);

    // Test move dir
    // Move EC dir under non-EC dir
    final Path newDir = new Path("/srcEC_new");
    fs.rename(srcECDir, newDir);
    fs.rename(newDir, srcECDir); // move back

    // Move EC dir under another EC dir
    fs.rename(srcECDir, dstECDir);
    fs.rename(new Path("/dstEC/srcEC"), srcECDir); // move back

    // Test move file
    /* Verify that a file can be moved between 2 EC dirs */
    fs.rename(srcFile, dstECDir);
    fs.rename(new Path(dstECDir, "foo"), srcECDir); // move back

    /* Verify that a file can be moved from a non-EC dir to an EC dir */
    final Path nonECDir = new Path("/nonEC");
    fs.mkdir(nonECDir, FsPermission.getDirDefault());
    fs.rename(srcFile, nonECDir);

    /* Verify that a file can be moved from an EC dir to a non-EC dir */
    final Path nonECFile = new Path(nonECDir, "nonECFile");
    fs.create(nonECFile);
    fs.rename(nonECFile, dstECDir);
  }

  @Test(timeout = 60000)
  public void testReplication() throws IOException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());
    fs.setErasureCodingPolicy(testDir, null);
    final Path fooFile = new Path(testDir, "foo");
    // create ec file with replication=0
    fs.create(fooFile, FsPermission.getFileDefault(), true,
        conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short)0, fs.getDefaultBlockSize(fooFile), null);
    ErasureCodingPolicy policy = fs.getErasureCodingPolicy(fooFile);
    // set replication should be a no-op
    fs.setReplication(fooFile, (short) 3);
    // should preserve the policy after set replication
    assertEquals(policy, fs.getErasureCodingPolicy(fooFile));
  }

  @Test(timeout = 60000)
  public void testGetErasureCodingPolicyWithSystemDefaultECPolicy() throws Exception {
    String src = "/ec";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir EC policy should be null
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir EC policy after setting
    fs.getClient().setErasureCodingPolicy(src, null); //Default one will be used.
    ErasureCodingPolicy sysDefaultECPolicy = ErasureCodingPolicyManager.getSystemDefaultPolicy();
    verifyErasureCodingInfo(src, sysDefaultECPolicy);
    fs.create(new Path(ecDir, "child1")).close();
    // verify for the files in ec dir
    verifyErasureCodingInfo(src + "/child1", sysDefaultECPolicy);
  }

  @Test(timeout = 60000)
  public void testGetErasureCodingPolicy() throws Exception {
    ErasureCodingPolicy[] sysECPolicies =
        ErasureCodingPolicyManager.getSystemPolicies();
    assertTrue("System ecPolicies should exist",
        sysECPolicies.length > 0);

    ErasureCodingPolicy usingECPolicy = sysECPolicies[0];
    String src = "/ec2";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir ECInfo before being set
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir ECInfo after set
    fs.getClient().setErasureCodingPolicy(src, usingECPolicy);
    verifyErasureCodingInfo(src, usingECPolicy);
    fs.create(new Path(ecDir, "child1")).close();
    // verify for the files in ec dir
    verifyErasureCodingInfo(src + "/child1", usingECPolicy);
  }

  private void verifyErasureCodingInfo(
      String src, ErasureCodingPolicy usingECPolicy) throws IOException {
    HdfsFileStatus hdfsFileStatus = fs.getClient().getFileInfo(src);
    ErasureCodingPolicy ecPolicy = hdfsFileStatus.getErasureCodingPolicy();
    assertNotNull(ecPolicy);
    assertEquals("Actually used ecPolicy should be equal with target ecPolicy",
        usingECPolicy, ecPolicy);
  }

  @Test(timeout = 60000)
  public void testCreationErasureCodingZoneWithInvalidPolicy()
      throws IOException {
    ECSchema rsSchema = new ECSchema("rs", 4, 2);
    String policyName = "RS-4-2-128k";
    int cellSize = 128 * 1024;
    ErasureCodingPolicy ecPolicy=
        new ErasureCodingPolicy(policyName, rsSchema, cellSize, (byte) -1);
    String src = "/ecDir4-2";
    final Path ecDir = new Path(src);
    try {
      fs.mkdir(ecDir, FsPermission.getDirDefault());
      fs.getClient().setErasureCodingPolicy(src, ecPolicy);
      fail("HadoopIllegalArgumentException should be thrown for"
          + "setting an invalid erasure coding policy");
    } catch (Exception e) {
      assertExceptionContains("Policy [ RS-4-2-128k ] does not match " +
          "any of the supported policies",e);
    }
  }

  @Test(timeout = 60000)
  public void testGetAllErasureCodingPolicies() throws Exception {
    ErasureCodingPolicy[] sysECPolicies = ErasureCodingPolicyManager
        .getSystemPolicies();
    Collection<ErasureCodingPolicy> allECPolicies = fs
        .getAllErasureCodingPolicies();
    assertTrue("All system policies should be active",
        allECPolicies.containsAll(Arrays.asList(sysECPolicies)));
  }

  @Test(timeout = 60000)
  public void testGetErasureCodingPolicyOnANonExistentFile() throws Exception {
    Path path = new Path("/ecDir");
    try {
      fs.getErasureCodingPolicy(path);
      fail("FileNotFoundException should be thrown for a non-existent"
          + " file path");
    } catch (FileNotFoundException e) {
      assertExceptionContains("Path not found: " + path, e);
    }
    HdfsAdmin dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    try {
      dfsAdmin.getErasureCodingPolicy(path);
      fail("FileNotFoundException should be thrown for a non-existent"
          + " file path");
    } catch (FileNotFoundException e) {
      assertExceptionContains("Path not found: " + path, e);
    }
  }

  @Test(timeout = 60000)
  public void testMultiplePoliciesCoExist() throws Exception {
    ErasureCodingPolicy[] sysPolicies =
        ErasureCodingPolicyManager.getSystemPolicies();
    if (sysPolicies.length > 1) {
      for (ErasureCodingPolicy policy : sysPolicies) {
        Path dir = new Path("/policy_" + policy.getId());
        fs.mkdir(dir, FsPermission.getDefault());
        fs.setErasureCodingPolicy(dir, policy);
        Path file = new Path(dir, "child");
        fs.create(file).close();
        assertEquals(policy, fs.getErasureCodingPolicy(file));
        assertEquals(policy, fs.getErasureCodingPolicy(dir));
        INode iNode = namesystem.getFSDirectory().getINode(file.toString());
        assertEquals(policy.getId(), iNode.asFile().getFileReplication());
      }
    }
  }
}
