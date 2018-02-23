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
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestViewFileSystemHdfs extends ViewFileSystemBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestViewFileSystemHdfs.class);


  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static Path defaultWorkingDirectory2;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem fHdfs2;
  private FileSystem fsTarget2;
  Path targetTestRoot2;
  
  @Override
  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper("/tmp/TestViewFileSystemHdfs");
  }

  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {

    // Encryption Zone settings
    FileSystemTestHelper fsHelper = new FileSystemTestHelper();
    String testRoot = fsHelper.getTestRootDir();
    CONF.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file" +
            new Path(new File(testRoot).getAbsoluteFile().toString(), "test" +
                ".jks").toUri());
    CONF.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        2);

    SupportsBlocks = true;
    CONF.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    
    cluster =
        new MiniDFSCluster.Builder(CONF).nnTopology(
                MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2)
            .build();
    cluster.waitClusterUp();
    
    fHdfs = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
    fHdfs.getConf().set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_URI.toString());
    fHdfs2.getConf().set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_URI.toString());

    defaultWorkingDirectory = fHdfs.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    defaultWorkingDirectory2 = fHdfs2.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    
    fHdfs.mkdirs(defaultWorkingDirectory);
    fHdfs2.mkdirs(defaultWorkingDirectory2);
  }

      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = fHdfs;
    fsTarget2 = fHdfs2;
    targetTestRoot2 = new FileSystemTestHelper().getAbsoluteTestRootPath(fsTarget2);
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  void setupMountPoints() {
    super.setupMountPoints();
    ConfigUtil.addLink(conf, "/mountOnNn2", new Path(targetTestRoot2,
        "mountOnNn2").toUri());
  }

  // Overriden test helper methods - changed values based on hdfs and the
  // additional mount.
  @Override
  int getExpectedDirPaths() {
    return 8;
  }
  
  @Override
  int getExpectedMountPoints() {
    return 9;
  }

  @Override
  int getExpectedDelegationTokenCount() {
    return 2; // Mount points to 2 unique hdfs 
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 2;
  }

  @Test
  public void testTrashRootsAfterEncryptionZoneDeletion() throws Exception {
    final Path zone = new Path("/EZ");
    fsTarget.mkdirs(zone);
    final Path zone1 = new Path("/EZ/zone1");
    fsTarget.mkdirs(zone1);

    DFSTestUtil.createKey("test_key", cluster, CONF);
    HdfsAdmin hdfsAdmin = new HdfsAdmin(cluster.getURI(0), CONF);
    final EnumSet<CreateEncryptionZoneFlag> provisionTrash =
        EnumSet.of(CreateEncryptionZoneFlag.PROVISION_TRASH);
    hdfsAdmin.createEncryptionZone(zone1, "test_key", provisionTrash);

    final Path encFile = new Path(zone1, "encFile");
    DFSTestUtil.createFile(fsTarget, encFile, 10240, (short) 1, 0xFEED);

    Configuration clientConf = new Configuration(CONF);
    clientConf.setLong(FS_TRASH_INTERVAL_KEY, 1);
    clientConf.set("fs.default.name", fsTarget.getUri().toString());
    FsShell shell = new FsShell(clientConf);

    //Verify file deletion within EZ
    DFSTestUtil.verifyDelete(shell, fsTarget, encFile, true);
    assertTrue("ViewFileSystem trash roots should include EZ file trash",
        (fsView.getTrashRoots(true).size() == 1));

    //Verify deletion of EZ
    DFSTestUtil.verifyDelete(shell, fsTarget, zone, true);
    assertTrue("ViewFileSystem trash roots should include EZ zone trash",
        (fsView.getTrashRoots(true).size() == 2));
  }

  @Test
  public void testDf() throws Exception {
    Configuration newConf = new Configuration(conf);

    // Verify if DF on non viewfs produces output as before, that is
    // without "Mounted On" header.
    DFSTestUtil.FsShellRun("-df", 0, "Use%" + System.lineSeparator(), newConf);

    // Setting the default Fs to viewfs
    newConf.set("fs.default.name", "viewfs:///");

    // Verify if DF on viewfs produces a new header "Mounted on"
    DFSTestUtil.FsShellRun("-df /user", 0, "Mounted on", newConf);

    DFSTestUtil.FsShellRun("-df viewfs:///user", 0, "/user", newConf);
    DFSTestUtil.FsShellRun("-df /user3", 1, "/user3", newConf);
    DFSTestUtil.FsShellRun("-df /user2/abc", 1, "No such file or directory",
        newConf);
    DFSTestUtil.FsShellRun("-df /user2/", 0, "/user2", newConf);
    DFSTestUtil.FsShellRun("-df /internalDir", 0, "/internalDir", newConf);
    DFSTestUtil.FsShellRun("-df /", 0, null, newConf);
    DFSTestUtil.FsShellRun("-df", 0, null, newConf);
  }

  @Test
  public void testFileChecksum() throws IOException {
    ViewFileSystem viewFs = (ViewFileSystem) fsView;
    Path mountDataRootPath = new Path("/data");
    String fsTargetFileName = "debug.log";
    Path fsTargetFilePath = new Path(targetTestRoot, "data/debug.log");
    Path mountDataFilePath = new Path(mountDataRootPath, fsTargetFileName);

    fileSystemTestHelper.createFile(fsTarget, fsTargetFilePath);
    FileStatus fileStatus = viewFs.getFileStatus(mountDataFilePath);
    long fileLength = fileStatus.getLen();

    FileChecksum fileChecksumViaViewFs =
        viewFs.getFileChecksum(mountDataFilePath);
    FileChecksum fileChecksumViaTargetFs =
        fsTarget.getFileChecksum(fsTargetFilePath);
    assertTrue("File checksum not matching!",
        fileChecksumViaViewFs.equals(fileChecksumViaTargetFs));

    fileChecksumViaViewFs =
        viewFs.getFileChecksum(mountDataFilePath, fileLength / 2);
    fileChecksumViaTargetFs =
        fsTarget.getFileChecksum(fsTargetFilePath, fileLength / 2);
    assertTrue("File checksum not matching!",
        fileChecksumViaViewFs.equals(fileChecksumViaTargetFs));
  }

  //Rename should fail on across different fileSystems
  @Test
  public void testRenameAccorssFilesystem() throws IOException {
    //data is mountpoint in nn1
    Path mountDataRootPath = new Path("/data");
    //mountOnNn2 is nn2 mountpoint
    Path fsTargetFilePath = new Path("/mountOnNn2");
    Path filePath = new Path(mountDataRootPath + "/ttest");
    Path hdfFilepath = new Path(fsTargetFilePath + "/ttest2");
    fsView.create(filePath);
    try {
      fsView.rename(filePath, hdfFilepath);
      ContractTestUtils.fail("Should thrown IOE on Renames across filesytems");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }

  @Test
  public void testNflyClosestRepair() throws Exception {
    testNflyRepair(NflyFSystem.NflyKey.repairOnRead);
  }

  @Test
  public void testNflyMostRecentRepair() throws Exception {
    testNflyRepair(NflyFSystem.NflyKey.readMostRecent);
  }

  private void testNflyRepair(NflyFSystem.NflyKey repairKey)
      throws Exception {
    LOG.info("Starting testNflyWriteSimpleFailover");
    final URI uri1 = targetTestRoot.toUri();
    final URI uri2 = targetTestRoot2.toUri();
    final URI[] testUris = new URI[] {
        new URI(uri1.getScheme(), uri1.getAuthority(), "/", null, null),
        new URI(uri2.getScheme(), uri2.getAuthority(), "/", null, null)
    };

    final Configuration testConf = new Configuration(conf);
    testConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);

    final String testString = "Hello Nfly!";
    final Path nflyRoot = new Path("/nflyroot");

    ConfigUtil.addLinkNfly(testConf,
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE,
        nflyRoot.toString(),
        "minReplication=2," + repairKey + "=true", testUris);

    final FileSystem nfly = FileSystem.get(URI.create("viewfs:///"), testConf);
    // wd = /nflyroot/user/<user>
    nfly.setWorkingDirectory(new Path(nflyRoot
        + nfly.getWorkingDirectory().toUri().getPath()));

    // 1. test mkdirs
    final Path testDir = new Path("testdir1/sub1/sub3");
    final Path testDir_tmp = new Path("testdir1/sub1/sub3_temp");
    assertTrue(testDir + ": Failed to create!", nfly.mkdirs(testDir));

    // Test renames
    assertTrue(nfly.rename(testDir, testDir_tmp));
    assertTrue(nfly.rename(testDir_tmp, testDir));

    for (final URI testUri : testUris) {
      final FileSystem fs = FileSystem.get(testUri, testConf);
      assertTrue(testDir + " should exist!", fs.exists(testDir));
    }

    // 2. test write
    final Path testFile = new Path("test.txt");
    final FSDataOutputStream fsDos = nfly.create(testFile);
    try {
      fsDos.writeUTF(testString);
    } finally {
      fsDos.close();
    }

    for (final URI testUri : testUris) {
      final FileSystem fs = FileSystem.get(testUri, testConf);
      final FSDataInputStream fsdis = fs.open(testFile);
      try {
        assertEquals("Wrong file content", testString, fsdis.readUTF());
      } finally {
        fsdis.close();
      }
    }

    // 3. test reads when one unavailable
    //
    // bring one NN down and read through nfly should still work
    //
    for (int i = 0; i < cluster.getNumNameNodes(); i++) {
      cluster.shutdownNameNode(i);
      FSDataInputStream fsDis = null;
      try {
        fsDis = nfly.open(testFile);
        assertEquals("Wrong file content", testString, fsDis.readUTF());
      } finally {
        IOUtils.cleanupWithLogger(LOG, fsDis);
        cluster.restartNameNode(i);
      }
    }

    // both nodes are up again, test repair
    final FileSystem fs1 = FileSystem.get(testUris[0], conf);
    assertTrue(fs1.delete(testFile, false));
    assertFalse(fs1.exists(testFile));
    FSDataInputStream fsDis = null;
    try {
      fsDis = nfly.open(testFile);
      assertEquals("Wrong file content", testString, fsDis.readUTF());
      assertTrue(fs1.exists(testFile));
    } finally {
      IOUtils.cleanupWithLogger(LOG, fsDis);
    }

    // test most recent repair
    if (repairKey == NflyFSystem.NflyKey.readMostRecent) {
      final FileSystem fs2 = FileSystem.get(testUris[0], conf);
      final long expectedMtime = fs2.getFileStatus(testFile)
          .getModificationTime();

      for (final URI testUri : testUris) {
        final FileSystem fs = FileSystem.get(testUri, conf);
        fs.setTimes(testFile, 1L, 1L);
        assertEquals(testUri + "Set mtime failed!", 1L,
            fs.getFileStatus(testFile).getModificationTime());
        assertEquals("nfly file status wrong", expectedMtime,
            nfly.getFileStatus(testFile).getModificationTime());
        FSDataInputStream fsDis2 = null;
        try {
          fsDis2 = nfly.open(testFile);
          assertEquals("Wrong file content", testString, fsDis2.readUTF());
          // repair is done, now trying via normal fs
          //
          assertEquals("Repair most recent failed!", expectedMtime,
              fs.getFileStatus(testFile).getModificationTime());
        } finally {
          IOUtils.cleanupWithLogger(LOG, fsDis2);
        }
      }
    }
  }
}
