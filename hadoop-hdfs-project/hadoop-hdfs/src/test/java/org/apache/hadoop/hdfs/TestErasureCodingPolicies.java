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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;

public class TestErasureCodingPolicies {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private static final ErasureCodingPolicy EC_POLICY =
      StripedFileTestUtil.getDefaultECPolicy();
  private FSNamesystem namesystem;

  @Rule
  public Timeout timeout = new Timeout(60 * 1000);

  @Before
  public void setupCluster() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    DFSTestUtil.enableAllECPolicies(conf);
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
  @Test
  public void testReplicatedFileUnderECDir() throws IOException {
    final Path dir = new Path("/ec");
    final Path replicatedFile = new Path(dir, "replicatedFile");
    // create a file with replicated blocks
    DFSTestUtil.createFile(fs, replicatedFile, 0, (short) 3, 0L);

    // set ec policy on dir
    fs.setErasureCodingPolicy(dir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
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

  @Test
  public void testBasicSetECPolicy()
      throws IOException, InterruptedException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());

    /* Normal creation of an erasure coding directory */
    fs.setErasureCodingPolicy(testDir, EC_POLICY.getName());

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
    fs.setErasureCodingPolicy(notEmpty, EC_POLICY.getName());
    final Path newFile = new Path(notEmpty, "new");
    fs.create(newFile);
    INode oldInode = namesystem.getFSDirectory().getINode(oldFile.toString());
    assertFalse(oldInode.asFile().isStriped());
    INode newInode = namesystem.getFSDirectory().getINode(newFile.toString());
    assertTrue(newInode.asFile().isStriped());

    /* Verify that nested EC policies are supported */
    final Path dir1 = new Path("/dir1");
    final Path dir2 = new Path(dir1, "dir2");
    fs.mkdir(dir1, FsPermission.getDirDefault());
    fs.setErasureCodingPolicy(dir1, EC_POLICY.getName());
    fs.mkdir(dir2, FsPermission.getDirDefault());
    try {
      fs.setErasureCodingPolicy(dir2, EC_POLICY.getName());
    } catch (IOException e) {
      fail("Nested erasure coding policies are supported");
    }

    /* Verify that EC policy cannot be set on a file */
    final Path fPath = new Path("/file");
    fs.create(fPath);
    try {
      fs.setErasureCodingPolicy(fPath, EC_POLICY.getName());
      fail("Erasure coding policy on file");
    } catch (IOException e) {
      assertExceptionContains("erasure coding policy for a file", e);
    }

    // Verify that policies are successfully loaded even when policies
    // are disabled
    cluster.getConfiguration(0).set(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY, "");
    cluster.restartNameNodes();
    cluster.waitActive();

    // No policies should be enabled after restart
    Assert.assertTrue("No policies should be enabled after restart",
        fs.getAllErasureCodingPolicies().isEmpty());

    // Already set directory-level policies should still be in effect
    Path disabledPolicy = new Path(dir1, "afterDisabled");
    Assert.assertEquals("Dir does not have policy set",
        EC_POLICY,
        fs.getErasureCodingPolicy(dir1));
    fs.create(disabledPolicy).close();
    Assert.assertEquals("File did not inherit dir's policy",
        EC_POLICY,
        fs.getErasureCodingPolicy(disabledPolicy));

    // Also check loading disabled EC policies from fsimage
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNodes();

    Assert.assertEquals("Dir does not have policy set",
        EC_POLICY,
        fs.getErasureCodingPolicy(dir1));
    Assert.assertEquals("File does not have policy set",
        EC_POLICY,
        fs.getErasureCodingPolicy(disabledPolicy));
  }

  @Test
  public void testMoveValidity() throws IOException, InterruptedException {
    final Path srcECDir = new Path("/srcEC");
    final Path dstECDir = new Path("/dstEC");
    fs.mkdir(srcECDir, FsPermission.getDirDefault());
    fs.mkdir(dstECDir, FsPermission.getDirDefault());
    fs.setErasureCodingPolicy(srcECDir, EC_POLICY.getName());
    fs.setErasureCodingPolicy(dstECDir, EC_POLICY.getName());
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

  @Test
  public void testReplication() throws IOException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());
    fs.setErasureCodingPolicy(testDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
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

  @Test
  public void testGetErasureCodingPolicyWithSystemDefaultECPolicy() throws Exception {
    String src = "/ec";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir EC policy should be null
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir EC policy after setting
    ErasureCodingPolicy sysDefaultECPolicy =
        StripedFileTestUtil.getDefaultECPolicy();
    fs.getClient().setErasureCodingPolicy(src, sysDefaultECPolicy.getName());
    verifyErasureCodingInfo(src, sysDefaultECPolicy);
    fs.create(new Path(ecDir, "child1")).close();
    // verify for the files in ec dir
    verifyErasureCodingInfo(src + "/child1", sysDefaultECPolicy);
  }

  @Test
  public void testGetErasureCodingPolicy() throws Exception {
    List<ErasureCodingPolicy> sysECPolicies =
        SystemErasureCodingPolicies.getPolicies();
    assertTrue("System ecPolicies should exist",
        sysECPolicies.size() > 0);

    ErasureCodingPolicy usingECPolicy = sysECPolicies.get(0);
    String src = "/ec2";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir ECInfo before being set
    assertNull(fs.getClient().getFileInfo(src).getErasureCodingPolicy());
    // dir ECInfo after set
    fs.getClient().setErasureCodingPolicy(src, usingECPolicy.getName());
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

  @Test
  public void testSetInvalidPolicy()
      throws IOException {
    ECSchema rsSchema = new ECSchema("rs", 4, 2);
    String policyName = "RS-4-2-128k";
    int cellSize = 128 * 1024;
    ErasureCodingPolicy ecPolicy =
        new ErasureCodingPolicy(policyName, rsSchema, cellSize, (byte) -1);
    String src = "/ecDir4-2";
    final Path ecDir = new Path(src);
    try {
      fs.mkdir(ecDir, FsPermission.getDirDefault());
      fs.getClient().setErasureCodingPolicy(src, ecPolicy.getName());
      fail("HadoopIllegalArgumentException should be thrown for"
          + "setting an invalid erasure coding policy");
    } catch (Exception e) {
      assertExceptionContains("Policy 'RS-4-2-128k' does not match " +
          "any enabled erasure coding policies", e);
    }
  }

  @Test
  public void testGetAllErasureCodingPolicies() throws Exception {
    Collection<ErasureCodingPolicy> allECPolicies = fs
        .getAllErasureCodingPolicies();
    assertTrue("All system policies should be enabled",
        allECPolicies.containsAll(SystemErasureCodingPolicies.getPolicies()));
  }

  @Test
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

  @Test
  public void testMultiplePoliciesCoExist() throws Exception {
    List<ErasureCodingPolicy> sysPolicies =
        SystemErasureCodingPolicies.getPolicies();
    if (sysPolicies.size() > 1) {
      for (ErasureCodingPolicy policy : sysPolicies) {
        Path dir = new Path("/policy_" + policy.getId());
        fs.mkdir(dir, FsPermission.getDefault());
        fs.setErasureCodingPolicy(dir, policy.getName());
        Path file = new Path(dir, "child");
        fs.create(file).close();
        assertEquals(policy, fs.getErasureCodingPolicy(file));
        assertEquals(policy, fs.getErasureCodingPolicy(dir));
        INode iNode = namesystem.getFSDirectory().getINode(file.toString());
        assertEquals(policy.getId(), iNode.asFile().getErasureCodingPolicyID());
        assertEquals(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
            iNode.asFile().getFileReplication());
      }
    }
  }

  @Test
  public void testPermissions() throws Exception {
    UserGroupInformation user =
        UserGroupInformation.createUserForTesting("ecuser",
            new String[]{"ecgroup"});
    FileSystem userfs = user.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });
    HdfsAdmin useradmin = user.doAs(new PrivilegedExceptionAction<HdfsAdmin>() {
      @Override
      public HdfsAdmin run() throws Exception {
        return new HdfsAdmin(userfs.getUri(), conf);
      }
    });

    // Create dir and set an EC policy, create an EC file
    Path ecdir = new Path("/ecdir");
    Path ecfile = new Path(ecdir, "ecfile");
    fs.setPermission(new Path("/"), new FsPermission((short)0777));
    userfs.mkdirs(ecdir);
    final String ecPolicyName = StripedFileTestUtil.getDefaultECPolicy()
        .getName();
    useradmin.setErasureCodingPolicy(ecdir, ecPolicyName);
    assertEquals("Policy not present on dir",
        ecPolicyName,
        useradmin.getErasureCodingPolicy(ecdir).getName());
    userfs.create(ecfile).close();
    assertEquals("Policy not present on file",
        ecPolicyName,
        useradmin.getErasureCodingPolicy(ecfile).getName());

    // Unset and re-set
    useradmin.unsetErasureCodingPolicy(ecdir);
    useradmin.setErasureCodingPolicy(ecdir, ecPolicyName);

    // Change write permissions and make sure set and unset are denied
    userfs.setPermission(ecdir, new FsPermission((short)0555));
    try {
      useradmin.setErasureCodingPolicy(ecdir, ecPolicyName);
      fail("Should not be able to setECPolicy without write permissions");
    } catch (AccessControlException e) {
      // pass
    }
    try {
      useradmin.unsetErasureCodingPolicy(ecdir);
      fail("Should not be able to unsetECPolicy without write permissions");
    } catch (AccessControlException e) {
      // pass
    }

    // Change the permissions again, check that set and unset work
    userfs.setPermission(ecdir, new FsPermission((short)0640));
    useradmin.unsetErasureCodingPolicy(ecdir);
    useradmin.setErasureCodingPolicy(ecdir, ecPolicyName);

    // Set, unset, and get with another user should be unauthorized
    UserGroupInformation nobody =
        UserGroupInformation.createUserForTesting("nobody",
            new String[]{"nogroup"});
    HdfsAdmin noadmin = nobody.doAs(new PrivilegedExceptionAction<HdfsAdmin>() {
      @Override
      public HdfsAdmin run() throws Exception {
        return new HdfsAdmin(userfs.getUri(), conf);
      }
    });
    try {
      noadmin.setErasureCodingPolicy(ecdir, ecPolicyName);
      fail("Should not be able to setECPolicy without write permissions");
    } catch (AccessControlException e) {
      // pass
    }
    try {
      noadmin.unsetErasureCodingPolicy(ecdir);
      fail("Should not be able to unsetECPolicy without write permissions");
    } catch (AccessControlException e) {
      // pass
    }
    try {
      noadmin.getErasureCodingPolicy(ecdir);
      fail("Should not be able to getECPolicy without write permissions");
    } catch (AccessControlException e) {
      // pass
    }

    // superuser can do whatever it wants
    userfs.setPermission(ecdir, new FsPermission((short)0000));
    HdfsAdmin superadmin = new HdfsAdmin(fs.getUri(), conf);
    superadmin.unsetErasureCodingPolicy(ecdir);
    superadmin.setErasureCodingPolicy(ecdir, ecPolicyName);
    superadmin.getErasureCodingPolicy(ecdir);

    // Normal user no longer has access
    try {
      useradmin.getErasureCodingPolicy(ecdir);
      fail("Normal user should not have access");
    } catch (AccessControlException e) {
      // pass
    }
    try {
      useradmin.setErasureCodingPolicy(ecfile, ecPolicyName);
      fail("Normal user should not have access");
    } catch (AccessControlException e) {
      // pass
    }
    try {
      useradmin.unsetErasureCodingPolicy(ecfile);
      fail("Normal user should not have access");
    } catch (AccessControlException e) {
      // pass
    }

    // Everyone has access to getting the list of EC policies
    useradmin.getErasureCodingPolicies();
    noadmin.getErasureCodingPolicies();
    superadmin.getErasureCodingPolicies();
  }

  /**
   * Test apply specific erasure coding policy on single file. Usually file's
   * policy is inherited from its parent.
   */
  @Test
  public void testFileLevelECPolicy() throws Exception {
    final Path dirPath = new Path("/striped");
    final Path filePath0 = new Path(dirPath, "file0");
    final Path filePath1 = new Path(dirPath, "file1");

    fs.mkdirs(dirPath);
    fs.setErasureCodingPolicy(dirPath, EC_POLICY.getName());

    // null EC policy name value means inheriting parent directory's policy
    fs.newFSDataOutputStreamBuilder(filePath0).build().close();
    ErasureCodingPolicy ecPolicyOnFile = fs.getErasureCodingPolicy(filePath0);
    assertEquals(EC_POLICY, ecPolicyOnFile);

    // Test illegal EC policy name
    final String illegalPolicyName = "RS-DEFAULT-1-2-64k";
    try {
      fs.newFSDataOutputStreamBuilder(filePath1)
          .setEcPolicyName(illegalPolicyName).build().close();
      Assert.fail("illegal erasure coding policy should not be found");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains("Policy '" + illegalPolicyName
          + "' does not match any enabled erasure coding policies", e);
    }
    fs.delete(dirPath, true);

    // Test create a file with a different EC policy than its parent directory
    fs.mkdirs(dirPath);
    final ErasureCodingPolicy ecPolicyOnDir =
        SystemErasureCodingPolicies.getByID(
            SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
    ecPolicyOnFile = EC_POLICY;
    fs.setErasureCodingPolicy(dirPath, ecPolicyOnDir.getName());
    fs.newFSDataOutputStreamBuilder(filePath0)
        .setEcPolicyName(ecPolicyOnFile.getName()).build().close();
    assertEquals(ecPolicyOnFile, fs.getErasureCodingPolicy(filePath0));
    assertEquals(ecPolicyOnDir, fs.getErasureCodingPolicy(dirPath));
    fs.delete(dirPath, true);
  }
}
