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
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

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
  private final Path topEZDir = new Path(rootDir, "topEZ");
  private final Path nestedEZDir = new Path(topEZDir, "nestedEZ");

  private final Path topEZBaseFile = new Path(rootDir, "topEZBaseFile");
  private Path topEZFile = new Path(topEZDir, "file");
  private Path topEZRawFile = new Path(rawDir, "topEZ/file");

  private final Path nestedEZBaseFile = new Path(rootDir, "nestedEZBaseFile");
  private Path nestedEZFile = new Path(nestedEZDir, "file");
  private Path nestedEZRawFile = new Path(rawDir, "topEZ/nestedEZ/file");

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
    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, getKeyProviderURI());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    // Lower the batch size for testing
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    Logger.getLogger(EncryptionZoneManager.class).setLevel(Level.TRACE);
    fs = cluster.getFileSystem();
    setProvider();

    // Create test keys and EZs
    DFSTestUtil.createKey(TOP_EZ_KEY, cluster, conf);
    DFSTestUtil.createKey(NESTED_EZ_KEY, cluster, conf);
    fs.mkdir(topEZDir, FsPermission.getDirDefault());
    fs.createEncryptionZone(topEZDir, TOP_EZ_KEY);
    fs.mkdir(nestedEZDir, FsPermission.getDirDefault());

    // Allow setting nested EZ temporarily
    cluster.getNamesystem().getFSDirectory().ezManager.setAllowNestedEZ();
    fs.createEncryptionZone(nestedEZDir, NESTED_EZ_KEY);
    cluster.getNamesystem().getFSDirectory().ezManager.setDisallowNestedEZ();

    DFSTestUtil.createFile(fs, topEZBaseFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, topEZFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, nestedEZBaseFile, len, (short) 1, 0xFEED);
    DFSTestUtil.createFile(fs, nestedEZFile, len, (short) 1, 0xFEED);
  }

  @Test(timeout = 60000)
  public void testNestedEncryptionZones() throws Exception {
    verifyEncryption();

    // Restart NameNode to test if nested EZs can be loaded from edit logs
    cluster.restartNameNodes();
    cluster.waitActive();
    verifyEncryption();

    // Checkpoint and restart NameNode, to test if nested EZs can be loaded
    // from fsimage
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNodes();
    cluster.waitActive();
    verifyEncryption();

    Path renamedTopEZFile = new Path(topEZDir, "renamedFile");
    Path renamedNestedEZFile = new Path(nestedEZDir, "renamedFile");
    try {
      fs.rename(topEZFile, renamedTopEZFile);
      fs.rename(nestedEZFile, renamedNestedEZFile);
    } catch (Exception e) {
      fail("Should be able to rename files within the same EZ.");
    }

    topEZFile = renamedTopEZFile;
    nestedEZFile = renamedNestedEZFile;
    topEZRawFile = new Path(rawDir, "topEZ/renamedFile");
    nestedEZRawFile = new Path(rawDir, "topEZ/nestedEZ/renamedFile");
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
      assertTrue(e.getMessage().contains(
          "can't be moved from an encryption zone"));
    }

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
          "can't be moved from encryption zone " + topEZ2Dir.toString() +
              " to encryption zone"));
    }

    try {
      fs.rename(topEZDir, new Path(rootDir, "newTopEZDir"));
    } catch (Exception e) {
      fail("Should be able to rename the root dir of an EZ.");
    }

    try {
      fs.rename(new Path(rootDir, "newTopEZDir/nestedEZDir"),
          new Path(rootDir, "newTopEZDir/newNestedEZDir"));
    } catch (Exception e) {
      fail("Should be able to rename the nested EZ dir within " +
          "the same top EZ.");
    }
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
