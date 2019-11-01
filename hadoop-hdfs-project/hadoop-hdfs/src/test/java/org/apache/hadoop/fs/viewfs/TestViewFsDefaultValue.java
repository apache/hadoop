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


import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for viewfs implementation of default fs level values.
 * This tests for both passing in a path (based on mount point)
 * to obtain the default value of the fs that the path is mounted on
 * or just passing in no arguments.
 */
public class TestViewFsDefaultValue {
  
  static final String testFileDir = "/tmp/test/";
  static final String testFileName = testFileDir + "testFileStatusSerialziation";
  static final String NOT_IN_MOUNTPOINT_FILENAME = "/NotInMountpointFile";
  private static MiniDFSCluster cluster;
  private static final FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper(); 
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem vfs;
  private static Path testFilePath;
  private static Path testFileDirPath;
  // Use NotInMountpoint path to trigger the exception
  private static Path notInMountpointPath;

  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {

    CONF.setLong(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    CONF.setInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT);
    CONF.setInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
      DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    CONF.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT + 1);
    CONF.setInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
 
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(DFS_REPLICATION_DEFAULT + 1).build();
    cluster.waitClusterUp();
    fHdfs = cluster.getFileSystem();
    fileSystemTestHelper.createFile(fHdfs, testFileName);
    fileSystemTestHelper.createFile(fHdfs, NOT_IN_MOUNTPOINT_FILENAME);
    Configuration conf = ViewFileSystemTestSetup.createConfig();
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT + 1);
    ConfigUtil.addLink(conf, "/tmp", new URI(fHdfs.getUri().toString() +
      "/tmp"));
    vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    testFileDirPath = new Path (testFileDir);
    testFilePath = new Path (testFileName);
    notInMountpointPath = new Path(NOT_IN_MOUNTPOINT_FILENAME);
  }


  /**
   * Test that default blocksize values can be retrieved on the client side.
   */
  @Test
  public void testGetDefaultBlockSize()
      throws IOException, URISyntaxException {
    // createFile does not use defaultBlockSize to create the file, 
    // but we are only looking at the defaultBlockSize, so this 
    // test should still pass
    try {
      vfs.getDefaultBlockSize(notInMountpointPath);
      fail("getServerDefaults on viewFs did not throw excetion!");
    } catch (NotInMountpointException e) {
      assertEquals(vfs.getDefaultBlockSize(testFilePath), 
        DFS_BLOCK_SIZE_DEFAULT);
    }
  }
  
  /**
   * Test that default replication values can be retrieved on the client side.
   */
  @Test
  public void testGetDefaultReplication()
      throws IOException, URISyntaxException {
    try {
      vfs.getDefaultReplication(notInMountpointPath);
      fail("getDefaultReplication on viewFs did not throw excetion!");
    } catch (NotInMountpointException e) {
      assertEquals(vfs.getDefaultReplication(testFilePath), 
        DFS_REPLICATION_DEFAULT+1);
    }
  }
 

  /**
   * Test that server default values can be retrieved on the client side.
   */
  @Test
  public void testServerDefaults() throws IOException {
    try {
      vfs.getServerDefaults(notInMountpointPath);
      fail("getServerDefaults on viewFs did not throw excetion!");
    } catch (NotInMountpointException e) {
      FsServerDefaults serverDefaults = vfs.getServerDefaults(testFilePath);
      assertEquals(DFS_BLOCK_SIZE_DEFAULT, serverDefaults.getBlockSize());
      assertEquals(DFS_BYTES_PER_CHECKSUM_DEFAULT,
        serverDefaults.getBytesPerChecksum());
      assertEquals(DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT,
        serverDefaults.getWritePacketSize());
      assertEquals(IO_FILE_BUFFER_SIZE_DEFAULT,
        serverDefaults.getFileBufferSize());
      assertEquals(DFS_REPLICATION_DEFAULT + 1,
        serverDefaults.getReplication());
    }
  }

  /**
   * Test that getContentSummary can be retrieved on the client side.
   */
  @Test
  public void testGetContentSummary() throws IOException {
    FileSystem hFs = cluster.getFileSystem(0);
    final DistributedFileSystem dfs = (DistributedFileSystem)hFs;
    dfs.setQuota(testFileDirPath, 100, 500);
    ContentSummary cs = vfs.getContentSummary(testFileDirPath);
    assertEquals(100, cs.getQuota()); 
    assertEquals(500, cs.getSpaceQuota()); 
  }

  /**
   * Test that getQuotaUsage can be retrieved on the client side.
   */
  @Test
  public void testGetQuotaUsage() throws IOException {
    FileSystem hFs = cluster.getFileSystem(0);
    final DistributedFileSystem dfs = (DistributedFileSystem)hFs;
    dfs.setQuota(testFileDirPath, 100, 500);
    QuotaUsage qu = vfs.getQuotaUsage(testFileDirPath);
    assertEquals(100, qu.getQuota());
    assertEquals(500, qu.getSpaceQuota());
  }

  /**
   * Test that getQuotaUsage can be retrieved on the client side if
   * storage types are defined.
   */
  @Test
  public void testGetQuotaUsageWithStorageTypes() throws IOException {
    FileSystem hFs = cluster.getFileSystem(0);
    final DistributedFileSystem dfs = (DistributedFileSystem)hFs;
    dfs.setQuotaByStorageType(testFileDirPath, StorageType.SSD, 500);
    dfs.setQuotaByStorageType(testFileDirPath, StorageType.DISK, 600);
    QuotaUsage qu = vfs.getQuotaUsage(testFileDirPath);
    assertEquals(500, qu.getTypeQuota(StorageType.SSD));
    assertEquals(600, qu.getTypeQuota(StorageType.DISK));
  }

  /**
   * Test that getQuotaUsage can be retrieved on the client side if
   * quota isn't defined.
   */
  @Test
  public void testGetQuotaUsageWithQuotaDefined() throws IOException {
    FileSystem hFs = cluster.getFileSystem(0);
    final DistributedFileSystem dfs = (DistributedFileSystem)hFs;
    dfs.setQuota(testFileDirPath, -1, -1);
    dfs.setQuotaByStorageType(testFileDirPath, StorageType.SSD, -1);
    dfs.setQuotaByStorageType(testFileDirPath, StorageType.DISK, -1);
    QuotaUsage qu = vfs.getQuotaUsage(testFileDirPath);
    assertEquals(-1, qu.getTypeQuota(StorageType.SSD));
    assertEquals(-1, qu.getQuota());
    assertEquals(-1, qu.getSpaceQuota());
    assertEquals(2, qu.getFileAndDirectoryCount());
    assertEquals(0, qu.getTypeConsumed(StorageType.SSD));
    assertTrue(qu.getSpaceConsumed() > 0);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    fHdfs.delete(new Path(testFileName), true);
    fHdfs.delete(notInMountpointPath, true);
  }

}
