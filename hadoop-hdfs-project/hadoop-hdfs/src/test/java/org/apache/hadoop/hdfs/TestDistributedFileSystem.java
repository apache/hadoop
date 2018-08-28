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

import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem.HdfsDataOutputStreamBuilder;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class TestDistributedFileSystem {
  private static final Random RAN = new Random();
  private static final Logger LOG = LoggerFactory.getLogger(
      TestDistributedFileSystem.class);

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(LeaseRenewer.LOG, Level.DEBUG);
  }

  private boolean dualPortTesting = false;
  
  private boolean noXmlDefaults = false;
  
  private HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf;
    if (noXmlDefaults) {
      conf = new HdfsConfiguration(false);
      String namenodeDir = new File(MiniDFSCluster.getBaseDirectory(), "name").
          getAbsolutePath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
    } else {
      conf = new HdfsConfiguration();
    }
    if (dualPortTesting) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          "localhost:0");
    }
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);

    return conf;
  }

  @Test
  public void testEmptyDelegationToken() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fileSys = cluster.getFileSystem();
      fileSys.getDelegationToken("");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).
        build();
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = getTestConfiguration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   * Also tests that any cached sockets are closed. (HDFS-3359)
   */
  @Test
  public void testDFSClose() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      DistributedFileSystem fileSys = cluster.getFileSystem();

      // create two files, leaving them open
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      // create another file, close it, and read it, so
      // the client gets a socket in its SocketCache
      Path p = new Path("/non-empty-file");
      DFSTestUtil.createFile(fileSys, p, 1L, (short)1, 0L);
      DFSTestUtil.readFile(fileSys, p);

      fileSys.close();

      DFSClient dfsClient = fileSys.getClient();
      verifyOpsUsingClosedClient(dfsClient);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private void verifyOpsUsingClosedClient(DFSClient dfsClient) {
    Path p = new Path("/non-empty-file");
    try {
      dfsClient.getBlockSize(p.getName());
      fail("getBlockSize using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getServerDefaults();
      fail("getServerDefaults using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.reportBadBlocks(new LocatedBlock[0]);
      fail("reportBadBlocks using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getBlockLocations(p.getName(), 0, 1);
      fail("getBlockLocations using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.createSymlink("target", "link", true);
      fail("createSymlink using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getLinkTarget(p.getName());
      fail("getLinkTarget using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setReplication(p.getName(), (short) 3);
      fail("setReplication using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setStoragePolicy(p.getName(),
          HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
      fail("setStoragePolicy using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getStoragePolicies();
      fail("getStoragePolicies using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      fail("setSafeMode using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.refreshNodes();
      fail("refreshNodes using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.metaSave(p.getName());
      fail("metaSave using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setBalancerBandwidth(1000L);
      fail("setBalancerBandwidth using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.finalizeUpgrade();
      fail("finalizeUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollingUpgrade(RollingUpgradeAction.QUERY);
      fail("rollingUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream();
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream(100L);
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.saveNamespace(1000L, 200L);
      fail("saveNamespace using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollEdits();
      fail("rollEdits using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.restoreFailedStorage("");
      fail("restoreFailedStorage using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getContentSummary(p.getName());
      fail("getContentSummary using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuota(p.getName(), 1000L, 500L);
      fail("setQuota using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuotaByStorageType(p.getName(), StorageType.DISK, 500L);
      fail("setQuotaByStorageType using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test
  public void testDFSCloseOrdering() throws Exception {
    DistributedFileSystem fs = new MyDistributedFileSystem();
    Path path = new Path("/a");
    fs.deleteOnExit(path);
    fs.close();

    InOrder inOrder = inOrder(fs.dfs);
    inOrder.verify(fs.dfs).closeOutputStreams(eq(false));
    inOrder.verify(fs.dfs).delete(eq(path.toString()), eq(true));
    inOrder.verify(fs.dfs).close();
  }
  
  private static class MyDistributedFileSystem extends DistributedFileSystem {
    MyDistributedFileSystem() {
      dfs = mock(DFSClient.class);
    }
    @Override
    public boolean exists(Path p) {
      return true; // trick out deleteOnExit
    }
    // Symlink resolution doesn't work with a mock, since it doesn't
    // have a valid Configuration to resolve paths to the right FileSystem.
    // Just call the DFSClient directly to register the delete
    @Override
    public boolean delete(Path f, final boolean recursive) throws IOException {
      return dfs.delete(f.toUri().getPath(), recursive);
    }
  }

  @Test
  public void testDFSSeekExceptions() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fileSys = cluster.getFileSystem();
      String file = "/test/fileclosethenseek/file-0";
      Path path = new Path(file);
      // create file
      FSDataOutputStream output = fileSys.create(path);
      output.writeBytes("Some test data to write longer than 10 bytes");
      output.close();
      FSDataInputStream input = fileSys.open(path);
      input.seek(10);
      boolean threw = false;
      try {
        input.seek(100);
      } catch (IOException e) {
        // success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking past end", threw);
      input.close();
      threw = false;
      try {
        input.seek(1);
      } catch (IOException e) {
        //success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking after close", threw);
      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSClient() throws Exception {
    Configuration conf = getTestConfiguration();
    final long grace = 1000L;
    MiniDFSCluster cluster = null;
    LeaseRenewer.setLeaseRenewerGraceDefault(grace);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      final String filepathstring = "/test/LeaseChecker/foo";
      final Path[] filepaths = new Path[4];
      for(int i = 0; i < filepaths.length; i++) {
        filepaths[i] = new Path(filepathstring + i);
      }
      final long millis = Time.now();

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean) checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
  
        {
          //create a file
          final FSDataOutputStream out = dfs.create(filepaths[0]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something
          out.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close
          out.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file1
          final FSDataOutputStream out1 = dfs.create(filepaths[1]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //create file2
          final FSDataOutputStream out2 = dfs.create(filepaths[2]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file1
          out1.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file1
          out1.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file2
          out2.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file2
          out2.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file3
          final FSDataOutputStream out3 = dfs.create(filepaths[3]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //passed previous grace period, should still running
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something to file3
          out3.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file3
          out3.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        dfs.close();
      }

      {
        // Check to see if opening a non-existent file triggers a FNF
        FileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/wrwelkj");
        assertFalse("File should not exist for test.", fs.exists(dir));

        try {
          FSDataInputStream in = fs.open(dir);
          try {
            in.close();
            fs.close();
          } finally {
            assertTrue("Did not get a FileNotFoundException for non-existing" +
                " file.", false);
          }
        } catch (FileNotFoundException fnf) {
          // This is the proper exception to catch; move on.
        }

      }

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

        //open and check the file
        FSDataInputStream in = dfs.open(filepaths[0]);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        assertEquals(millis, in.readLong());
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        in.close();
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        dfs.close();
      }
      
      { // test accessing DFS with ip address. should work with any hostname
        // alias or ip address that points to the interface that NameNode
        // is listening on. In this case, it is localhost.
        String uri = "hdfs://127.0.0.1:" + cluster.getNameNodePort() + 
                      "/test/ipAddress/file";
        Path path = new Path(uri);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream out = fs.create(path);
        byte[] buf = new byte[1024];
        out.write(buf);
        out.close();
        
        FSDataInputStream in = fs.open(path);
        in.readFully(buf);
        in.close();
        fs.close();
      }

      {
        // Test PathIsNotEmptyDirectoryException while deleting non-empty dir
        FileSystem fs = cluster.getFileSystem();
        fs.mkdirs(new Path("/test/nonEmptyDir"));
        fs.create(new Path("/tmp/nonEmptyDir/emptyFile")).close();
        try {
          fs.delete(new Path("/tmp/nonEmptyDir"), false);
          Assert.fail("Expecting PathIsNotEmptyDirectoryException");
        } catch (PathIsNotEmptyDirectoryException ex) {
          // This is the proper exception to catch; move on.
        }
        Assert.assertTrue(fs.exists(new Path("/test/nonEmptyDir")));
        fs.delete(new Path("/tmp/nonEmptyDir"), true);
      }

    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * This is to test that the {@link FileSystem#clearStatistics()} resets all
   * the global storage statistics.
   */
  @Test
  public void testClearStatistics() throws Exception {
    final Configuration conf = getTestConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      FileSystem dfs = cluster.getFileSystem();

      final Path dir = new Path("/testClearStatistics");
      final long mkdirCount = getOpStatistics(OpType.MKDIRS);
      long writeCount = DFSTestUtil.getStatistics(dfs).getWriteOps();
      dfs.mkdirs(dir);
      checkOpStatistics(OpType.MKDIRS, mkdirCount + 1);
      assertEquals(++writeCount,
          DFSTestUtil.getStatistics(dfs).getWriteOps());

      final long createCount = getOpStatistics(OpType.CREATE);
      FSDataOutputStream out = dfs.create(new Path(dir, "tmpFile"), (short)1);
      out.write(40);
      out.close();
      checkOpStatistics(OpType.CREATE, createCount + 1);
      assertEquals(++writeCount,
          DFSTestUtil.getStatistics(dfs).getWriteOps());

      FileSystem.clearStatistics();
      checkOpStatistics(OpType.MKDIRS, 0);
      checkOpStatistics(OpType.CREATE, 0);
      checkStatistics(dfs, 0, 0, 0);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testStatistics() throws IOException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class).reset();
    @SuppressWarnings("unchecked")
    ThreadLocal<StatisticsData> data = (ThreadLocal<StatisticsData>)
        Whitebox.getInternalState(
        FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class), "threadData");
    data.set(null);

    int lsLimit = 2;
    final Configuration conf = getTestConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, lsLimit);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      final FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/test");
      Path file = new Path(dir, "file");

      int readOps = 0;
      int writeOps = 0;
      int largeReadOps = 0;

      long opCount = getOpStatistics(OpType.MKDIRS);
      fs.mkdirs(dir);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.MKDIRS, opCount + 1);
      
      opCount = getOpStatistics(OpType.CREATE);
      FSDataOutputStream out = fs.create(file, (short)1);
      out.close();
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.CREATE, opCount + 1);

      opCount = getOpStatistics(OpType.GET_FILE_STATUS);
      FileStatus status = fs.getFileStatus(file);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_STATUS, opCount + 1);
      
      opCount = getOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS);
      fs.getFileBlockLocations(file, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS, opCount + 1);
      fs.getFileBlockLocations(status, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS, opCount + 2);
      
      opCount = getOpStatistics(OpType.OPEN);
      FSDataInputStream in = fs.open(file);
      in.close();
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.OPEN, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_REPLICATION);
      fs.setReplication(file, (short)2);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_REPLICATION, opCount + 1);
      
      opCount = getOpStatistics(OpType.RENAME);
      Path file1 = new Path(dir, "file1");
      fs.rename(file, file1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.RENAME, opCount + 1);
      
      opCount = getOpStatistics(OpType.GET_CONTENT_SUMMARY);
      fs.getContentSummary(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_CONTENT_SUMMARY, opCount + 1);
      
      
      // Iterative ls test
      long mkdirOp = getOpStatistics(OpType.MKDIRS);
      long listStatusOp = getOpStatistics(OpType.LIST_STATUS);
      long locatedListStatusOP = getOpStatistics(OpType.LIST_LOCATED_STATUS);
      for (int i = 0; i < 10; i++) {
        Path p = new Path(dir, Integer.toString(i));
        fs.mkdirs(p);
        mkdirOp++;
        FileStatus[] list = fs.listStatus(dir);
        if (list.length > lsLimit) {
          // if large directory, then count readOps and largeReadOps by 
          // number times listStatus iterates
          int iterations = (int)Math.ceil((double)list.length/lsLimit);
          largeReadOps += iterations;
          readOps += iterations;
          listStatusOp += iterations;
        } else {
          // Single iteration in listStatus - no large read operation done
          readOps++;
          listStatusOp++;
        }
        
        // writeOps incremented by 1 for mkdirs
        // readOps and largeReadOps incremented by 1 or more
        checkStatistics(fs, readOps, ++writeOps, largeReadOps);
        checkOpStatistics(OpType.MKDIRS, mkdirOp);
        checkOpStatistics(OpType.LIST_STATUS, listStatusOp);

        fs.listLocatedStatus(dir);
        locatedListStatusOP++;
        readOps++;
        checkStatistics(fs, readOps, writeOps, largeReadOps);
        checkOpStatistics(OpType.LIST_LOCATED_STATUS, locatedListStatusOP);
      }
      
      opCount = getOpStatistics(OpType.GET_STATUS);
      fs.getStatus(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_STATUS, opCount + 1);

      opCount = getOpStatistics(OpType.GET_FILE_CHECKSUM);
      fs.getFileChecksum(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_CHECKSUM, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_PERMISSION);
      fs.setPermission(file1, new FsPermission((short)0777));
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_PERMISSION, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_TIMES);
      fs.setTimes(file1, 0L, 0L);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_TIMES, opCount + 1);

      opCount = getOpStatistics(OpType.SET_OWNER);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      fs.setOwner(file1, ugi.getUserName(), ugi.getGroupNames()[0]);
      checkOpStatistics(OpType.SET_OWNER, opCount + 1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);

      opCount = getOpStatistics(OpType.DELETE);
      fs.delete(dir, true);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.DELETE, opCount + 1);
      
    } finally {
      if (cluster != null) cluster.shutdown();
    }
    
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Test (timeout = 180000)
  public void testConcurrentStatistics()
      throws IOException, InterruptedException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class).reset();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(
        new Configuration()).build();
    cluster.waitActive();
    final FileSystem fs = cluster.getFileSystem();
    final int numThreads = 5;
    final ExecutorService threadPool =
        HadoopExecutors.newFixedThreadPool(numThreads);

    try {
      final CountDownLatch allExecutorThreadsReady =
          new CountDownLatch(numThreads);
      final CountDownLatch startBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      final AtomicReference<Throwable> childError = new AtomicReference<>();

      for (int i = 0; i < numThreads; i++) {
        threadPool.submit(new Runnable() {
          @Override
          public void run() {
            allExecutorThreadsReady.countDown();
            try {
              startBlocker.await();
              final FileSystem fs = cluster.getFileSystem();
              fs.mkdirs(new Path("/testStatisticsParallelChild"));
            } catch (Throwable t) {
              LOG.error("Child failed when calling mkdir", t);
              childError.compareAndSet(null, t);
            } finally {
              allDone.countDown();
            }
          }
        });
      }

      final long oldMkdirOpCount = getOpStatistics(OpType.MKDIRS);

      // wait until all threads are ready
      allExecutorThreadsReady.await();
      // all threads start making directories
      startBlocker.countDown();
      // wait until all threads are done
      allDone.await();

     assertNull("Child failed with exception " + childError.get(),
          childError.get());

      checkStatistics(fs, 0, numThreads, 0);
      // check the single operation count stat
      checkOpStatistics(OpType.MKDIRS, numThreads + oldMkdirOpCount);
      // iterate all the operation counts
      for (Iterator<LongStatistic> opCountIter =
           FileSystem.getGlobalStorageStatistics()
               .get(DFSOpsCountStatistics.NAME).getLongStatistics();
           opCountIter.hasNext();) {
        final LongStatistic opCount = opCountIter.next();
        if (OpType.MKDIRS.getSymbol().equals(opCount.getName())) {
          assertEquals("Unexpected op count from iterator!",
              numThreads + oldMkdirOpCount, opCount.getValue());
        }
        LOG.info(opCount.getName() + "\t" + opCount.getValue());
      }
    } finally {
      threadPool.shutdownNow();
      cluster.shutdown();
    }
  }

  /** Checks statistics. -1 indicates do not check for the operations */
  private void checkStatistics(FileSystem fs, int readOps, int writeOps,
      int largeReadOps) {
    assertEquals(readOps, DFSTestUtil.getStatistics(fs).getReadOps());
    assertEquals(writeOps, DFSTestUtil.getStatistics(fs).getWriteOps());
    assertEquals(largeReadOps, DFSTestUtil.getStatistics(fs).getLargeReadOps());
  }

  /** Checks read statistics. */
  private void checkReadStatistics(FileSystem fs, int distance, long expectedReadBytes) {
    long bytesRead = DFSTestUtil.getStatistics(fs).
        getBytesReadByDistance(distance);
    assertEquals(expectedReadBytes, bytesRead);
  }

  @Test
  public void testLocalHostReadStatistics() throws Exception {
    testReadFileSystemStatistics(0, false, false);
  }

  @Test
  public void testLocalRackReadStatistics() throws Exception {
    testReadFileSystemStatistics(2, false, false);
  }

  @Test
  public void testRemoteRackOfFirstDegreeReadStatistics() throws Exception {
    testReadFileSystemStatistics(4, false, false);
  }

  @Test
  public void testInvalidScriptMappingFileReadStatistics() throws Exception {
    // Even though network location of the client machine is unknown,
    // MiniDFSCluster's datanode is on the local host and thus the network
    // distance is 0.
    testReadFileSystemStatistics(0, true, true);
  }

  @Test
  public void testEmptyScriptMappingFileReadStatistics() throws Exception {
    // Network location of the client machine is resolved to
    // {@link NetworkTopology#DEFAULT_RACK} when there is no script file
    // defined. This is equivalent to unknown network location.
    // MiniDFSCluster's datanode is on the local host and thus the network
    // distance is 0.
    testReadFileSystemStatistics(0, true, false);
  }

  /** expectedDistance is the expected distance between client and dn.
   * 0 means local host.
   * 2 means same rack.
   * 4 means remote rack of first degree.
   * invalidScriptMappingConfig is used to test
   */
  private void testReadFileSystemStatistics(int expectedDistance,
      boolean useScriptMapping, boolean invalidScriptMappingFile)
      throws IOException {
    MiniDFSCluster cluster = null;
    StaticMapping.addNodeToRack(NetUtils.getLocalHostname(), "/rackClient");
    final Configuration conf = getTestConfiguration();
    conf.setBoolean(FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED, true);
    // ClientContext is cached globally by default thus we will end up using
    // the network distance computed by other test cases.
    // Use different value for DFS_CLIENT_CONTEXT in each test case so that it
    // can compute network distance independently.
    conf.set(DFS_CLIENT_CONTEXT, "testContext_" + expectedDistance);

    // create a cluster with a dn with the expected distance.
    // MiniDFSCluster by default uses StaticMapping unless the test
    // overrides it.
    if (useScriptMapping) {
      conf.setClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          ScriptBasedMapping.class, DNSToSwitchMapping.class);
      if (invalidScriptMappingFile) {
        conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
            "invalidScriptFile.txt");
      }
      cluster = new MiniDFSCluster.Builder(conf).
          useConfiguredTopologyMappingClass(true).build();
    } else if (expectedDistance == 0) {
      cluster = new MiniDFSCluster.Builder(conf).
          hosts(new String[] {NetUtils.getLocalHostname()}).build();
    } else if (expectedDistance == 2) {
      cluster = new MiniDFSCluster.Builder(conf).
          racks(new String[]{"/rackClient"}).build();
    } else if (expectedDistance == 4) {
      cluster = new MiniDFSCluster.Builder(conf).
          racks(new String[]{"/rackFoo"}).build();
    }

    // create a file, read the file and verify the metrics
    try {
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.getStatistics(fs).reset();
      Path dir = new Path("/test");
      Path file = new Path(dir, "file");
      String input = "hello world";
      DFSTestUtil.writeFile(fs, file, input);
      FSDataInputStream stm = fs.open(file);
      byte[] actual = new byte[4096];
      stm.read(actual);
      checkReadStatistics(fs, expectedDistance, input.length());
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  private static void checkOpStatistics(OpType op, long count) {
    assertEquals("Op " + op.getSymbol() + " has unexpected count!",
        count, getOpStatistics(op));
  }

  private static long getOpStatistics(OpType op) {
    return GlobalStorageStatistics.INSTANCE.get(
        DFSOpsCountStatistics.NAME)
        .getLong(op.getSymbol());
  }

  @Test
  public void testFileChecksum() throws Exception {
    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    final Configuration conf = getTestConfiguration();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    final FileSystem hdfs = cluster.getFileSystem();

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    
    try {
      hdfs.getFileChecksum(new Path(
          "/test/TestNonExistingFile"));
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("File does not exist: /test/TestNonExistingFile"));
    }

    try {
      Path path = new Path("/test/TestExistingDir/");
      hdfs.mkdirs(path);
      hdfs.getFileChecksum(path);
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("Path is not a file: /test/TestExistingDir"));
    }

    //webhdfs
    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(webhdfsuri).getFileSystem(conf);
      }
    });

    final Path dir = new Path("/filechecksum");
    final int block_size = 1024;
    final int buffer_size = conf.getInt(
        CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
    conf.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

    //try different number of blocks
    for(int n = 0; n < 5; n++) {
      //generate random data
      final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
      RAN.nextBytes(data);
      System.out.println("data.length=" + data.length);
  
      //write data to a file
      final Path foo = new Path(dir, "foo" + n);
      {
        final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
      
      //compute checksum
      final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
      System.out.println("hdfsfoocs=" + hdfsfoocs);

      //webhdfs
      final FileChecksum webhdfsfoocs = webhdfs.getFileChecksum(foo);
      System.out.println("webhdfsfoocs=" + webhdfsfoocs);

      final Path webhdfsqualified = new Path(webhdfsuri + dir, "foo" + n);
      final FileChecksum webhdfs_qfoocs =
          webhdfs.getFileChecksum(webhdfsqualified);
      System.out.println("webhdfs_qfoocs=" + webhdfs_qfoocs);

      //create a zero byte file
      final Path zeroByteFile = new Path(dir, "zeroByteFile" + n);
      {
        final FSDataOutputStream out = hdfs.create(zeroByteFile, false,
            buffer_size, (short)2, block_size);
        out.close();
      }

      //write another file
      final Path bar = new Path(dir, "bar" + n);
      {
        final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }

      {
        final FileChecksum zeroChecksum = hdfs.getFileChecksum(zeroByteFile);
        final String magicValue =
            "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51";
        // verify the magic val for zero byte files
        assertEquals(magicValue, zeroChecksum.toString());

        //verify checksums for empty file and 0 request length
        final FileChecksum checksumWith0 = hdfs.getFileChecksum(bar, 0);
        assertEquals(zeroChecksum, checksumWith0);

        //verify checksum
        final FileChecksum barcs = hdfs.getFileChecksum(bar);
        final int barhashcode = barcs.hashCode();
        assertEquals(hdfsfoocs.hashCode(), barhashcode);
        assertEquals(hdfsfoocs, barcs);

        //webhdfs
        assertEquals(webhdfsfoocs.hashCode(), barhashcode);
        assertEquals(webhdfsfoocs, barcs);

        assertEquals(webhdfs_qfoocs.hashCode(), barhashcode);
        assertEquals(webhdfs_qfoocs, barcs);
      }

      hdfs.setPermission(dir, new FsPermission((short)0));

      { //test permission error on webhdfs 
        try {
          webhdfs.getFileChecksum(webhdfsqualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }
      hdfs.setPermission(dir, new FsPermission((short)0777));
    }
    cluster.shutdown();
  }
  
  @Test
  public void testAllWithDualPort() throws Exception {
    dualPortTesting = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
      dualPortTesting = false;
    }
  }
  
  @Test
  public void testAllWithNoXmlDefaults() throws Exception {
    // Do all the tests with a configuration that ignores the defaults in
    // the XML files.
    noXmlDefaults = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
     noXmlDefaults = false; 
    }
  }

  @Test(timeout=120000)
  public void testLocatedFileStatusStorageIdsTypes() throws Exception {
    final Configuration conf = getTestConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    try {
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path testFile = new Path("/testListLocatedStatus");
      final int blockSize = 4096;
      final int numBlocks = 10;
      // Create a test file
      final int repl = 2;
      DFSTestUtil.createFile(fs, testFile, blockSize, numBlocks * blockSize,
          blockSize, (short) repl, 0xADDED);
      DFSTestUtil.waitForReplication(fs, testFile, (short) repl, 30000);
      // Get the listing
      RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(testFile);
      assertTrue("Expected file to be present", it.hasNext());
      LocatedFileStatus stat = it.next();
      BlockLocation[] locs = stat.getBlockLocations();
      assertEquals("Unexpected number of locations", numBlocks, locs.length);

      Set<String> dnStorageIds = new HashSet<>();
      for (DataNode d : cluster.getDataNodes()) {
        try (FsDatasetSpi.FsVolumeReferences volumes = d.getFSDataset()
            .getFsVolumeReferences()) {
          for (FsVolumeSpi vol : volumes) {
            dnStorageIds.add(vol.getStorageID());
          }
        }
      }

      for (BlockLocation loc : locs) {
        String[] ids = loc.getStorageIds();
        // Run it through a set to deduplicate, since there should be no dupes
        Set<String> storageIds = new HashSet<>();
        Collections.addAll(storageIds, ids);
        assertEquals("Unexpected num storage ids", repl, storageIds.size());
        // Make sure these are all valid storage IDs
        assertTrue("Unknown storage IDs found!", dnStorageIds.containsAll
            (storageIds));
        // Check storage types are the default, since we didn't set any
        StorageType[] types = loc.getStorageTypes();
        assertEquals("Unexpected num storage types", repl, types.length);
        for (StorageType t: types) {
          assertEquals("Unexpected storage type", StorageType.DEFAULT, t);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCreateWithCustomChecksum() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    Path testBasePath = new Path("/test/csum");
    // create args 
    Path path1 = new Path(testBasePath, "file_wtih_crc1");
    Path path2 = new Path(testBasePath, "file_with_crc2");
    ChecksumOpt opt1 = new ChecksumOpt(DataChecksum.Type.CRC32C, 512);
    ChecksumOpt opt2 = new ChecksumOpt(DataChecksum.Type.CRC32, 512);

    // common args
    FsPermission perm = FsPermission.getDefault().applyUMask(
        FsPermission.getUMask(conf));
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.OVERWRITE,
        CreateFlag.CREATE);
    short repl = 1;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(testBasePath);

      // create two files with different checksum types
      FSDataOutputStream out1 = dfs.create(path1, perm, flags, 4096, repl,
          131072L, null, opt1);
      FSDataOutputStream out2 = dfs.create(path2, perm, flags, 4096, repl,
          131072L, null, opt2);

      for (int i = 0; i < 1024; i++) {
        out1.write(i);
        out2.write(i);
      }
      out1.close();
      out2.close();

      // the two checksums must be different.
      MD5MD5CRC32FileChecksum sum1 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path1);
      MD5MD5CRC32FileChecksum sum2 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path2);
      assertFalse(sum1.equals(sum2));

      // check the individual params
      assertEquals(DataChecksum.Type.CRC32C, sum1.getCrcType());
      assertEquals(DataChecksum.Type.CRC32,  sum2.getCrcType());

    } finally {
      if (cluster != null) {
        cluster.getFileSystem().delete(testBasePath, true);
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=60000)
  public void testFileCloseStatus() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // create a new file.
      Path file = new Path("/simpleFlush.dat");
      FSDataOutputStream output = fs.create(file);
      // write to file
      output.writeBytes("Some test data");
      output.flush();
      assertFalse("File status should be open", fs.isFileClosed(file));
      output.close();
      assertTrue("File status should be closed", fs.isFileClosed(file));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=60000)
  public void testListFiles() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
  
      final Path relative = new Path("relative");
      fs.create(new Path(relative, "foo")).close();
  
      final List<LocatedFileStatus> retVal = new ArrayList<>();
      final RemoteIterator<LocatedFileStatus> iter =
          fs.listFiles(relative, true);
      while (iter.hasNext()) {
        retVal.add(iter.next());
      }
      System.out.println("retVal = " + retVal);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testListStatusOfSnapshotDirs() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.create(new Path("/parent/test1/dfsclose/file-0"));
      Path snapShotDir = new Path("/parent/test1/");
      dfs.allowSnapshot(snapShotDir);

      FileStatus status = dfs.getFileStatus(new Path("/parent/test1"));
      assertTrue(status.isSnapshotEnabled());
      status = dfs.getFileStatus(new Path("/parent/"));
      assertFalse(status.isSnapshotEnabled());
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerReadTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();     
      DistributedFileSystem dfs = cluster.getFileSystem();
      // use a dummy socket to ensure the read timesout
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
          (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        peer.getInputStream().read();
        Assert.fail("read should timeout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;
        if (delta < timeout*0.9) {
          throw new IOException("read timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout*1.1) {
          throw new IOException("read timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testGetServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FsServerDefaults fsServerDefaults = dfs.getServerDefaults();
      Assert.assertNotNull(fsServerDefaults);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerWriteTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Write 10 MB to a dummy socket to ensure the write times out
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
        (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        byte[] buf = new byte[10 * 1024 * 1024];
        peer.getOutputStream().write(buf);
        long delta = Time.now() - start;
        Assert.fail("write finish in " + delta + " ms" + "but should timedout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;

        if (delta < timeout * 0.9) {
          throw new IOException("write timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout * 1.2) {
          throw new IOException("write timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testTotalDfsUsed() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      // create file under root
      FSDataOutputStream File1 = fs.create(new Path("/File1"));
      File1.write("hi".getBytes());
      File1.close();
      // create file under sub-folder
      FSDataOutputStream File2 = fs.create(new Path("/Folder1/File2"));
      File2.write("hi".getBytes());
      File2.close();
      // getUsed(Path) should return total len of all the files from a path
      assertEquals(2, fs.getUsed(new Path("/Folder1")));
      //getUsed() should return total length of all files in filesystem
      assertEquals(4, fs.getUsed());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @Test
  public void testDFSCloseFilesBeingWritten() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      DistributedFileSystem fileSys = cluster.getFileSystem();

      // Create one file then delete it to trigger the FileNotFoundException
      // when closing the file.
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.delete(new Path("/test/dfsclose/file-0"), true);

      DFSClient dfsClient = fileSys.getClient();
      // Construct a new dfsClient to get the same LeaseRenewer instance,
      // to avoid the original client being added to the leaseRenewer again.
      DFSClient newDfsClient =
          new DFSClient(cluster.getFileSystem(0).getUri(), conf);
      LeaseRenewer leaseRenewer = newDfsClient.getLeaseRenewer();

      dfsClient.closeAllFilesBeingWritten(false);
      // Remove new dfsClient in leaseRenewer
      leaseRenewer.closeClient(newDfsClient);

      // The list of clients corresponding to this renewer should be empty
      assertEquals(true, leaseRenewer.isEmpty());
      assertEquals(true, dfsClient.isFilesBeingWrittenEmpty());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testBuilderSetters(DistributedFileSystem fs) {
    Path testFilePath = new Path("/testBuilderSetters");
    HdfsDataOutputStreamBuilder builder = fs.createFile(testFilePath);

    builder.append().overwrite(false).newBlock().lazyPersist().noLocalWrite()
        .ecPolicyName("ec-policy");
    EnumSet<CreateFlag> flags = builder.getFlags();
    assertTrue(flags.contains(CreateFlag.APPEND));
    assertTrue(flags.contains(CreateFlag.CREATE));
    assertTrue(flags.contains(CreateFlag.NEW_BLOCK));
    assertTrue(flags.contains(CreateFlag.NO_LOCAL_WRITE));
    assertFalse(flags.contains(CreateFlag.OVERWRITE));
    assertFalse(flags.contains(CreateFlag.SYNC_BLOCK));

    assertEquals("ec-policy", builder.getEcPolicyName());
    assertFalse(builder.shouldReplicate());
  }

  @Test
  public void testHdfsDataOutputStreamBuilderSetParameters()
      throws IOException {
    Configuration conf = getTestConfiguration();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      testBuilderSetters(fs);
    }
  }

  @Test
  public void testDFSDataOutputStreamBuilderForCreation() throws Exception {
    Configuration conf = getTestConfiguration();
    String testFile = "/testDFSDataOutputStreamBuilder";
    Path testFilePath = new Path(testFile);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      DistributedFileSystem fs = cluster.getFileSystem();

      // Before calling build(), no change was made in the file system
      HdfsDataOutputStreamBuilder builder = fs.createFile(testFilePath)
          .blockSize(4096).replication((short)1);
      assertFalse(fs.exists(testFilePath));

      // Test create an empty file
      try (FSDataOutputStream out =
               fs.createFile(testFilePath).build()) {
        LOG.info("Test create an empty file");
      }

      // Test create a file with content, and verify the content
      String content = "This is a test!";
      try (FSDataOutputStream out1 = fs.createFile(testFilePath)
          .bufferSize(4096)
          .replication((short) 1)
          .blockSize(4096)
          .build()) {
        byte[] contentOrigin = content.getBytes("UTF8");
        out1.write(contentOrigin);
      }

      ContractTestUtils.verifyFileContents(fs, testFilePath,
          content.getBytes());

      try (FSDataOutputStream out = fs.createFile(testFilePath).overwrite(false)
        .build()) {
        fail("it should fail to overwrite an existing file");
      } catch (FileAlreadyExistsException e) {
        // As expected, ignore.
      }

      Path nonParentFile = new Path("/parent/test");
      try (FSDataOutputStream out = fs.createFile(nonParentFile).build()) {
        fail("parent directory not exist");
      } catch (FileNotFoundException e) {
        // As expected.
      }
      assertFalse("parent directory should not be created",
          fs.exists(new Path("/parent")));

      try (FSDataOutputStream out = fs.createFile(nonParentFile).recursive()
        .build()) {
        out.write(1);
      }
      assertTrue("parent directory has not been created",
          fs.exists(new Path("/parent")));
    }
  }

  @Test
  public void testDFSDataOutputStreamBuilderForAppend() throws IOException {
    Configuration conf = getTestConfiguration();
    String testFile = "/testDFSDataOutputStreamBuilderForAppend";
    Path path = new Path(testFile);
    Random random = new Random();
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build()) {
      DistributedFileSystem fs = cluster.getFileSystem();

      byte[] buf = new byte[16];
      random.nextBytes(buf);

      try (FSDataOutputStream out = fs.appendFile(path).build()) {
        out.write(buf);
        fail("should fail on appending to non-existent file");
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains("non-existent", e);
      }

      random.nextBytes(buf);
      try (FSDataOutputStream out = fs.createFile(path).build()) {
        out.write(buf);
      }

      random.nextBytes(buf);
      try (FSDataOutputStream out = fs.appendFile(path).build()) {
        out.write(buf);
      }

      FileStatus status = fs.getFileStatus(path);
      assertEquals(16 * 2, status.getLen());
    }
  }

  @Test
  public void testRemoveErasureCodingPolicy() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      DistributedFileSystem fs = cluster.getFileSystem();
      ECSchema toAddSchema = new ECSchema("rs", 3, 2);
      ErasureCodingPolicy toAddPolicy =
          new ErasureCodingPolicy(toAddSchema, 128 * 1024, (byte) 254);
      String policyName = toAddPolicy.getName();
      ErasureCodingPolicy[] policies = new ErasureCodingPolicy[]{toAddPolicy};
      fs.addErasureCodingPolicies(policies);
      assertEquals(policyName, ErasureCodingPolicyManager.getInstance().
          getByName(policyName).getName());
      fs.removeErasureCodingPolicy(policyName);
      assertEquals(policyName, ErasureCodingPolicyManager.getInstance().
          getRemovedPolicies().get(0).getName());

      // remove erasure coding policy as a user without privilege
      UserGroupInformation fakeUGI = UserGroupInformation.createUserForTesting(
          "ProbablyNotARealUserName", new String[] {"ShangriLa"});
      final MiniDFSCluster finalCluster = cluster;
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          DistributedFileSystem fs = finalCluster.getFileSystem();
          try {
            fs.removeErasureCodingPolicy(policyName);
            fail();
          } catch (AccessControlException ace) {
            GenericTestUtils.assertExceptionContains("Access denied for user " +
                "ProbablyNotARealUserName. Superuser privilege is required",
                ace);
          }
          return null;
        }
      });

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnableAndDisableErasureCodingPolicy() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      DistributedFileSystem fs = cluster.getFileSystem();
      ECSchema toAddSchema = new ECSchema("rs", 3, 2);
      ErasureCodingPolicy toAddPolicy =
          new ErasureCodingPolicy(toAddSchema, 128 * 1024, (byte) 254);
      String policyName = toAddPolicy.getName();
      ErasureCodingPolicy[] policies =
          new ErasureCodingPolicy[]{toAddPolicy};
      fs.addErasureCodingPolicies(policies);
      assertEquals(policyName, ErasureCodingPolicyManager.getInstance().
          getByName(policyName).getName());
      fs.disableErasureCodingPolicy(policyName);
      fs.enableErasureCodingPolicy(policyName);
      assertEquals(policyName, ErasureCodingPolicyManager.getInstance().
          getByName(policyName).getName());

      //test enable a policy that doesn't exist
      try {
        fs.enableErasureCodingPolicy("notExistECName");
        Assert.fail("enable the policy that doesn't exist should fail");
      } catch (Exception e) {
        GenericTestUtils.assertExceptionContains("does not exist", e);
        // pass
      }

      //test disable a policy that doesn't exist
      try {
        fs.disableErasureCodingPolicy("notExistECName");
        Assert.fail("disable the policy that doesn't exist should fail");
      } catch (Exception e) {
        GenericTestUtils.assertExceptionContains("does not exist", e);
        // pass
      }

      // disable and enable erasure coding policy as a user without privilege
      UserGroupInformation fakeUGI = UserGroupInformation.createUserForTesting(
          "ProbablyNotARealUserName", new String[] {"ShangriLa"});
      final MiniDFSCluster finalCluster = cluster;
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          DistributedFileSystem fs = finalCluster.getFileSystem();
          try {
            fs.disableErasureCodingPolicy(policyName);
            fail();
          } catch (AccessControlException ace) {
            GenericTestUtils.assertExceptionContains("Access denied for user " +
                    "ProbablyNotARealUserName. Superuser privilege is required",
                ace);
          }
          try {
            fs.enableErasureCodingPolicy(policyName);
            fail();
          } catch (AccessControlException ace) {
            GenericTestUtils.assertExceptionContains("Access denied for user " +
                    "ProbablyNotARealUserName. Superuser privilege is required",
                ace);
          }
          return null;
        }
      });
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
