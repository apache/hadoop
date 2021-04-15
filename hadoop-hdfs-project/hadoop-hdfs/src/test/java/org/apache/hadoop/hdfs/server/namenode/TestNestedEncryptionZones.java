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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the behavior of nested encryption zones.
 */
public class TestNestedEncryptionZones {
  private File testRootDir;
  private final String TOP_EZ_KEY = "topezkey";
  private final String NESTED_EZ_KEY = "nestedezkey";

  private MiniDFSCluster cluster;
  protected DistributedFileSystem fs;

  private final Path rootDir = new Path("/");
  private final Path rawDir = new Path("/.reserved/raw/");

  private Path nestedEZBaseFile = new Path(rootDir, "nestedEZBaseFile");
  private Path topEZBaseFile = new Path(rootDir, "topEZBaseFile");

  private Path topEZDir;
  private Path nestedEZDir;

  private Path topEZFile;
  private Path nestedEZFile;

  private Path topEZRawFile;
  private Path nestedEZRawFile;


  // File length
  private final int len = 8196;

  private String getKeyProviderURI() {
    return JavaKeyStoreProvider.SCHEME_NAME + "://file" +
        new Path(testRootDir.toString(), "test.jks").toUri();
  }

  private void setProvider() {
    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    fs.getClient().setKeyProvider(cluster.getNameNode().getNamesystem()
        .getProvider());
  }

  @Before
  public void setup() throws Exception {
    Configuration conf = new HdfsConfiguration();
    FileSystemTestHelper fsHelper = new FileSystemTestHelper();
    // Set up java key store
    String testRoot = fsHelper.getTestRootDir();
    testRootDir = new File(testRoot).getAbsoluteFile();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    // Lower the batch size for testing
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        2);
    // enable trash for testing
    conf.setLong(DFSConfigKeys.FS_TRASH_INTERVAL_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(EncryptionZoneManager.class), Level.TRACE);
    fs = cluster.getFileSystem();
    setProvider();

    // Create test keys and EZs
    DFSTestUtil.createKey(TOP_EZ_KEY, cluster, conf);
    DFSTestUtil.createKey(NESTED_EZ_KEY, cluster, conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testNestedEncryptionZones() throws Exception {
    initTopEZDirAndNestedEZDir(new Path(rootDir, "topEZ"));
    verifyEncryption();

    // Restart NameNode to test if nested EZs can be loaded from edit logs
    cluster.restartNameNodes();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    verifyEncryption();

    // Checkpoint and restart NameNode, to test if nested EZs can be loaded
    // from fsimage
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNodes();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    verifyEncryption();

    renameChildrenOfEZ();

    // Verify that a non-nested EZ cannot be moved into another EZ
    Path topEZ2Dir = new Path(rootDir, "topEZ2");
    fs.mkdir(topEZ2Dir, FsPermission.getDirDefault());
    fs.createEncryptionZone(topEZ2Dir, TOP_EZ_KEY);
    try {
      fs.rename(topEZ2Dir, new Path(topEZDir, "topEZ2"));
      fail("Shouldn't be able to move a non-nested EZ into another " +
          "existing EZ.");
    } catch (Exception e){
      assertTrue(e.getMessage().contains(
          "can't be moved into an encryption zone"));
    }

    // Should be able to rename the root dir of an EZ.
    fs.rename(topEZDir, new Path(rootDir, "newTopEZ"));

    // Should be able to rename the nested EZ dir within the same top EZ.
    fs.rename(new Path(rootDir, "newTopEZ/nestedEZ"),
        new Path(rootDir, "newTopEZ/newNestedEZ"));
  }

  @Test(timeout = 60000)
  public void testNestedEZWithRoot() throws Exception {
    initTopEZDirAndNestedEZDir(rootDir);
    verifyEncryption();

    // test rename file
    renameChildrenOfEZ();

    final String currentUser =
        UserGroupInformation.getCurrentUser().getShortUserName();
    final Path suffixTrashPath = new Path(
        FileSystem.TRASH_PREFIX, currentUser);

    final Path rootTrash = fs.getTrashRoot(rootDir);
    final Path topEZTrash = fs.getTrashRoot(topEZFile);
    final Path nestedEZTrash = fs.getTrashRoot(nestedEZFile);

    final Path expectedTopEZTrash = fs.makeQualified(
        new Path(topEZDir, suffixTrashPath));
    final Path expectedNestedEZTrash = fs.makeQualified(
        new Path(nestedEZDir, suffixTrashPath));

    assertEquals("Top ez trash should be " + expectedTopEZTrash,
        expectedTopEZTrash, topEZTrash);
    assertEquals("Root trash should be equal with TopEZFile trash",
        topEZTrash, rootTrash);
    assertEquals("Nested ez Trash should be " + expectedNestedEZTrash,
        expectedNestedEZTrash, nestedEZTrash);

    // delete rename file and test trash
    FsShell shell = new FsShell(fs.getConf());
    final Path topTrashFile = new Path(
        shell.getCurrentTrashDir(topEZFile) + "/" + topEZFile);
    final Path nestedTrashFile = new Path(
        shell.getCurrentTrashDir(nestedEZFile) + "/" + nestedEZFile);

    ToolRunner.run(shell, new String[]{"-rm", topEZFile.toString()});
    ToolRunner.run(shell, new String[]{"-rm", nestedEZFile.toString()});

    assertTrue("File not in trash : " + topTrashFile, fs.exists(topTrashFile));
    assertTrue(
        "File not in trash : " + nestedTrashFile, fs.exists(nestedTrashFile));
  }

  @Test(timeout = 60000)
  public void testRenameBetweenEncryptionZones() throws Exception {
    String key1 = TOP_EZ_KEY;
    String key2 = NESTED_EZ_KEY;
    Path top = new Path("/dir");
    Path ez1 = new Path(top, "ez1");
    Path ez2 = new Path(top, "ez2");
    Path ez3 = new Path(top, "ez3");
    Path p = new Path(ez1, "file");
    fs.mkdirs(ez1, FsPermission.getDirDefault());
    fs.mkdirs(ez2, FsPermission.getDirDefault());
    fs.mkdirs(ez3, FsPermission.getDirDefault());
    fs.createEncryptionZone(ez1, key1);
    fs.createEncryptionZone(ez2, key2);
    fs.createEncryptionZone(ez3, key1);
    fs.create(p).close();

    // cannot rename between 2 EZs with different keys.
    try {
      fs.rename(p, new Path(ez2, "file"));
    } catch (RemoteException re) {
      Assert.assertEquals(
          p + " can't be moved from encryption zone " + ez1 +
              " to encryption zone " + ez2 + ".",
          re.getMessage().split("\n")[0]);
    }
    // can rename between 2 EZs with the same key.
    Assert.assertTrue(fs.rename(p, new Path(ez3, "file")));
  }

  @Test(timeout = 60000)
  public void testRemoveEncryptionZoneWithAncestorKey() throws Exception {
    removeEZDirUnderAncestor(TOP_EZ_KEY);
  }

  @Test(timeout = 60000)
  public void testRemoveEncryptionZoneWithNoAncestorKey() throws Exception {
    removeEZDirUnderAncestor(null);
  }

  private void removeEZDirUnderAncestor(String parentKey) throws Exception {
    String[] dirs = new String[]{"/dir", "ez1", "ez2"};
    String[] ezKeys = new String[]{parentKey, TOP_EZ_KEY, NESTED_EZ_KEY};
    Path[] pathArr = new Path[3];

    // initialize the directories.
    for (int i = 0; i < dirs.length; i++) {
      pathArr[i] = (i == 0 ? new Path(dirs[i]) : new Path(pathArr[0], dirs[i]));
      fs.mkdirs(pathArr[i], FsPermission.getDirDefault());
      if (ezKeys[i] == null) { // skip creating encryption zone.
        continue;
      }
      fs.createEncryptionZone(pathArr[i], ezKeys[i]);
    }

    // try to remove the EZ.
    // can't remove EZ when no ancestor shares same key.
    for (int i = 1; i < dirs.length; i++) {
      removeEZ(pathArr[i], !ezKeys[i].equals(parentKey));
    }
  }

  private void removeEZ(Path p, boolean expectFailure) throws IOException {
    Path rawp = new Path(rawDir.toString() + p);
    try {
      fs.removeXAttr(rawp, HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE);
      Assert.assertFalse("didn't fail as expected", expectFailure);
    } catch (RemoteException re) {
      Assert.assertEquals(
          "The encryption zone xattr should never be deleted.",
          re.getMessage().split("\n")[0]);
    }
  }

  private void renameChildrenOfEZ() throws Exception{
    Path renamedTopEZFile = new Path(topEZDir, "renamedFile");
    Path renamedNestedEZFile = new Path(nestedEZDir, "renamedFile");

    //Should be able to rename files within the same EZ.
    fs.rename(topEZFile, renamedTopEZFile);
    fs.rename(nestedEZFile, renamedNestedEZFile);

    topEZFile = renamedTopEZFile;
    nestedEZFile = renamedNestedEZFile;
    topEZRawFile = new Path(rawDir + topEZFile.toUri().getPath());
    nestedEZRawFile = new Path(rawDir + nestedEZFile.toUri().getPath());
    verifyEncryption();

    // Verify that files in top EZ cannot be moved into the nested EZ, and
    // vice versa.
    try {
      fs.rename(topEZFile, new Path(nestedEZDir, "movedTopEZFile"));
      fail("Shouldn't be able to rename between top EZ and nested EZ.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(
          "can't be moved from encryption zone " + topEZDir.toString() +
              " to encryption zone " + nestedEZDir.toString()));
    }
    try {
      fs.rename(nestedEZFile, new Path(topEZDir, "movedNestedEZFile"));
      fail("Shouldn't be able to rename between top EZ and nested EZ.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(
          "can't be moved from encryption zone " + nestedEZDir.toString() +
              " to encryption zone " + topEZDir.toString()));
    }

    // Verify that the nested EZ cannot be moved out of the top EZ.
    try {
      fs.rename(nestedEZFile, new Path(rootDir, "movedNestedEZFile"));
      fail("Shouldn't be able to move the nested EZ out of the top EZ.");
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      assertTrue(exceptionMsg.contains(
          "can't be moved from") && exceptionMsg.contains("encryption zone"));
    }
  }

  private void initTopEZDirAndNestedEZDir(Path topPath) throws Exception {

    // init fs root directory
    fs.delete(rootDir, true);


    // init top and nested path or file
    topEZDir = topPath;
    nestedEZDir = new Path(topEZDir, "nestedEZ");

    topEZFile = new Path(topEZDir, "file");
    nestedEZFile = new Path(nestedEZDir, "file");

    topEZRawFile = new Path(rawDir + topEZFile.toUri().getPath());
    nestedEZRawFile = new Path(rawDir + nestedEZFile.toUri().getPath());

    // create ez zone
    fs.mkdir(topEZDir, FsPermission.getDirDefault());
    fs.createEncryptionZone(topEZDir, TOP_EZ_KEY);
    fs.mkdir(nestedEZDir, FsPermission.getDirDefault());
    fs.createEncryptionZone(nestedEZDir, NESTED_EZ_KEY);

    // create files
    DFSTestUtil.createFile(fs, topEZBaseFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, topEZFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, nestedEZBaseFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, nestedEZFile, len, (short) 1, 0xFEED);

  }

  private void verifyEncryption() throws Exception {
    assertEquals("Top EZ dir is encrypted",
        true, fs.getFileStatus(topEZDir).isEncrypted());
    assertEquals("Nested EZ dir is encrypted",
        true, fs.getFileStatus(nestedEZDir).isEncrypted());
    assertEquals("Top zone file is encrypted",
        true, fs.getFileStatus(topEZFile).isEncrypted());
    assertEquals("Nested zone file is encrypted",
        true, fs.getFileStatus(nestedEZFile).isEncrypted());

    DFSTestUtil.verifyFilesEqual(fs, topEZBaseFile, topEZFile, len);
    DFSTestUtil.verifyFilesEqual(fs, nestedEZBaseFile, nestedEZFile, len);
    DFSTestUtil.verifyFilesNotEqual(fs, topEZRawFile, nestedEZRawFile, len);
  }
}
