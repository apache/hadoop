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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

public class TestLeaseRecovery2 {
  
  public static final Log LOG = LogFactory.getLog(TestLeaseRecovery2.class);
  
  {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(LeaseManager.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.TRACE);
  }

  static final private long BLOCK_SIZE = 1024;
  static final private int FILE_SIZE = (int)BLOCK_SIZE*2;
  static final short REPLICATION_NUM = (short)3;
  static final byte[] buffer = new byte[FILE_SIZE];
  
  static private final String fakeUsername = "fakeUser1";
  static private final String fakeGroup = "supergroup";

  static private MiniDFSCluster cluster;
  static private DistributedFileSystem dfs;
  final static private Configuration conf = new HdfsConfiguration();
  final static private int BUF_SIZE = conf.getInt(
      CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
  
  final static private long SHORT_LEASE_PERIOD = 1000L;
  final static private long LONG_LEASE_PERIOD = 60*60*SHORT_LEASE_PERIOD;
  
  /** start a dfs cluster
   * 
   * @throws IOException
   */
  @Before
  public void startUp() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(5)
        .checkExitOnShutdown(false)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }
  
  /**
   * stop the cluster
   * @throws IOException
   */
  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
  }

  /**
   * Test the NameNode's revoke lease on current lease holder function.
   * @throws Exception
   */
  @Test
  public void testImmediateRecoveryOfLease() throws Exception {
    //create a file
    // write bytes into the file.
    byte [] actual = new byte[FILE_SIZE];
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    Path filepath = createFile("/immediateRecoverLease-shortlease", size, true);
    // set the soft limit to be 1 second so that the
    // namenode triggers lease recovery on next attempt to write-for-open.
    cluster.setLeasePeriod(SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);

    recoverLeaseUsingCreate(filepath);
    verifyFile(dfs, filepath, actual, size);

    //test recoverLease
    // set the soft limit to be 1 hour but recoverLease should
    // close the file immediately
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, LONG_LEASE_PERIOD);
    size = AppendTestUtil.nextInt(FILE_SIZE);
    filepath = createFile("/immediateRecoverLease-longlease", size, false);

    // test recoverLease from a different client
    recoverLease(filepath, null);
    verifyFile(dfs, filepath, actual, size);

    // test recoverlease from the same client
    size = AppendTestUtil.nextInt(FILE_SIZE);
    filepath = createFile("/immediateRecoverLease-sameclient", size, false);

    // create another file using the same client
    Path filepath1 = new Path(filepath.toString() + AppendTestUtil.nextInt());
    FSDataOutputStream stm = dfs.create(filepath1, true, BUF_SIZE,
      REPLICATION_NUM, BLOCK_SIZE);

    // recover the first file
    recoverLease(filepath, dfs);
    verifyFile(dfs, filepath, actual, size);

    // continue to write to the second file
    stm.write(buffer, 0, size);
    stm.close();
    verifyFile(dfs, filepath1, actual, size);
  }

  @Test
  public void testLeaseRecoverByAnotherUser() throws Exception {
    byte [] actual = new byte[FILE_SIZE];
    cluster.setLeasePeriod(SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);
    Path filepath = createFile("/immediateRecoverLease-x", 0, true);
    recoverLeaseUsingCreate2(filepath);
    verifyFile(dfs, filepath, actual, 0);
  }

  private Path createFile(final String filestr, final int size,
      final boolean triggerLeaseRenewerInterrupt)
  throws IOException, InterruptedException {
    AppendTestUtil.LOG.info("filestr=" + filestr);
    Path filepath = new Path(filestr);
    FSDataOutputStream stm = dfs.create(filepath, true, BUF_SIZE,
      REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(filestr));

    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();

    if (triggerLeaseRenewerInterrupt) {
      AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
      dfs.dfs.getLeaseRenewer().interruptAndJoin();
    }
    return filepath;
  }

  private void recoverLease(Path filepath, DistributedFileSystem dfs)
  throws Exception {
    if (dfs == null) {
      dfs = (DistributedFileSystem)getFSAsAnotherUser(conf);
    }

    while (!dfs.recoverLease(filepath)) {
      AppendTestUtil.LOG.info("sleep " + 5000 + "ms");
      Thread.sleep(5000);
    }
  }

  private FileSystem getFSAsAnotherUser(final Configuration c)
  throws IOException, InterruptedException {
    return FileSystem.get(FileSystem.getDefaultUri(c), c,
      UserGroupInformation.createUserForTesting(fakeUsername, 
          new String [] {fakeGroup}).getUserName());
  }

  private void recoverLeaseUsingCreate(Path filepath)
      throws IOException, InterruptedException {
    FileSystem dfs2 = getFSAsAnotherUser(conf);
    for(int i = 0; i < 10; i++) {
      AppendTestUtil.LOG.info("i=" + i);
      try {
        dfs2.create(filepath, false, BUF_SIZE, (short)1, BLOCK_SIZE);
        fail("Creation of an existing file should never succeed.");
      } catch(FileAlreadyExistsException e) {
        return; // expected
      } catch(AlreadyBeingCreatedException e) {
        return; // expected
      } catch(IOException ioe) {
        AppendTestUtil.LOG.warn("UNEXPECTED ", ioe);
        AppendTestUtil.LOG.info("sleep " + 5000 + "ms");
        try {Thread.sleep(5000);} catch (InterruptedException e) {}
      }
    }
    fail("recoverLeaseUsingCreate failed");
  }

  private void recoverLeaseUsingCreate2(Path filepath)
          throws Exception {
    FileSystem dfs2 = getFSAsAnotherUser(conf);
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    DistributedFileSystem dfsx = (DistributedFileSystem) dfs2;
    //create file using dfsx
    Path filepath2 = new Path("/immediateRecoverLease-x2");
    FSDataOutputStream stm = dfsx.create(filepath2, true, BUF_SIZE,
        REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfsx.dfs.exists("/immediateRecoverLease-x2"));
    try {Thread.sleep(10000);} catch (InterruptedException e) {}
    dfsx.append(filepath);
  }

  private void verifyFile(FileSystem dfs, Path filepath, byte[] actual,
      int size) throws IOException {
    AppendTestUtil.LOG.info("Lease for file " +  filepath + " is recovered. "
        + "Validating its contents now...");

    // verify that file-size matches
    assertTrue("File should be " + size + " bytes, but is actually " +
               " found to be " + dfs.getFileStatus(filepath).getLen() +
               " bytes",
               dfs.getFileStatus(filepath).getLen() == size);

    // verify that there is enough data to read.
    System.out.println("File size is good. Now validating sizes from datanodes...");
    FSDataInputStream stmin = dfs.open(filepath);
    stmin.readFully(0, actual, 0, size);
    stmin.close();
  }

  /**
   * This test makes the client does not renew its lease and also
   * set the hard lease expiration period to be short 1s. Thus triggering
   * lease expiration to happen while the client is still alive.
   * 
   * The test makes sure that the lease recovery completes and the client
   * fails if it continues to write to the file.
   * 
   * @throws Exception
   */
  @Test
  public void testHardLeaseRecovery() throws Exception {
    //create a file
    String filestr = "/hardLeaseRecovery";
    AppendTestUtil.LOG.info("filestr=" + filestr);
    Path filepath = new Path(filestr);
    FSDataOutputStream stm = dfs.create(filepath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(filestr));

    // write bytes into the file.
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    
    // kill the lease renewal thread
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.getLeaseRenewer().interruptAndJoin();

    // set the hard limit to be 1 second 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      Thread.sleep(SHORT_LEASE_PERIOD);
      locatedBlocks = dfs.dfs.getLocatedBlocks(filestr, 0L, size);
    } while (locatedBlocks.isUnderConstruction());
    assertEquals(size, locatedBlocks.getFileLength());

    // make sure that the writer thread gets killed
    try {
      stm.write('b');
      stm.close();
      fail("Writer thread should have been killed");
    } catch (IOException e) {
      e.printStackTrace();
    }      

    // verify data
    AppendTestUtil.LOG.info(
        "File size is good. Now validating sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filepath, size, buffer, filestr);
  }
  
  /**
   * This test makes the client does not renew its lease and also
   * set the soft lease expiration period to be short 1s. Thus triggering
   * soft lease expiration to happen immediately by having another client
   * trying to create the same file.
   * 
   * The test makes sure that the lease recovery completes.
   * 
   * @throws Exception
   */
  @Test
  public void testSoftLeaseRecovery() throws Exception {
    Map<String, String []> u2g_map = new HashMap<String, String []>(1);
    u2g_map.put(fakeUsername, new String[] {fakeGroup});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);

    // Reset default lease periods
    cluster.setLeasePeriod(HdfsConstants.LEASE_SOFTLIMIT_PERIOD,
                           HdfsConstants.LEASE_HARDLIMIT_PERIOD);
    //create a file
    // create a random file name
    String filestr = "/foo" + AppendTestUtil.nextInt();
    AppendTestUtil.LOG.info("filestr=" + filestr);
    Path filepath = new Path(filestr);
    FSDataOutputStream stm = dfs.create(filepath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(filestr));

    // write random number of bytes into it.
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.getLeaseRenewer().interruptAndJoin();

    // set the soft limit to be 1 second so that the
    // namenode triggers lease recovery on next attempt to write-for-open.
    cluster.setLeasePeriod(SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);

    // try to re-open the file before closing the previous handle. This
    // should fail but will trigger lease recovery.
    {
      UserGroupInformation ugi = 
        UserGroupInformation.createUserForTesting(fakeUsername, 
            new String [] { fakeGroup});

      FileSystem dfs2 = DFSTestUtil.getFileSystemAs(ugi, conf);

      boolean done = false;
      for(int i = 0; i < 10 && !done; i++) {
        AppendTestUtil.LOG.info("i=" + i);
        try {
          dfs2.create(filepath, false, BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
          fail("Creation of an existing file should never succeed.");
        } catch (FileAlreadyExistsException ex) {
          done = true;
        } catch (AlreadyBeingCreatedException ex) {
          AppendTestUtil.LOG.info("GOOD! got " + ex.getMessage());
        } catch (IOException ioe) {
          AppendTestUtil.LOG.warn("UNEXPECTED IOException", ioe);
        }

        if (!done) {
          AppendTestUtil.LOG.info("sleep " + 5000 + "ms");
          try {Thread.sleep(5000);} catch (InterruptedException e) {}
        }
      }
      assertTrue(done);
    }

    AppendTestUtil.LOG.info("Lease for file " +  filepath + " is recovered. "
        + "Validating its contents now...");

    // verify that file-size matches
    long fileSize = dfs.getFileStatus(filepath).getLen();
    assertTrue("File should be " + size + " bytes, but is actually " +
        " found to be " + fileSize + " bytes", fileSize == size);

    // verify data
    AppendTestUtil.LOG.info("File size is good. " +
                     "Now validating data and sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filepath, size, buffer, filestr);
  }
  
  /**
   * This test makes it so the client does not renew its lease and also
   * set the hard lease expiration period to be short, thus triggering
   * lease expiration to happen while the client is still alive. The test
   * also causes the NN to restart after lease recovery has begun, but before
   * the DNs have completed the blocks. This test verifies that when the NN
   * comes back up, the client no longer holds the lease.
   * 
   * The test makes sure that the lease recovery completes and the client
   * fails if it continues to write to the file, even after NN restart.
   * 
   * @throws Exception
   */
  @Test(timeout = 30000)
  public void testHardLeaseRecoveryAfterNameNodeRestart() throws Exception {
    hardLeaseRecoveryRestartHelper(false, -1);
  }

  @Test(timeout = 30000)
  public void testHardLeaseRecoveryAfterNameNodeRestart2() throws Exception {
    hardLeaseRecoveryRestartHelper(false, 1535);
  }

  @Test(timeout = 30000)
  public void testHardLeaseRecoveryWithRenameAfterNameNodeRestart()
      throws Exception {
    hardLeaseRecoveryRestartHelper(true, -1);
  }
  
  public void hardLeaseRecoveryRestartHelper(boolean doRename, int size)
      throws Exception {
    if (size < 0) {
      size =  AppendTestUtil.nextInt(FILE_SIZE + 1);
    }

    //create a file
    String fileStr = "/hardLeaseRecovery";
    AppendTestUtil.LOG.info("filestr=" + fileStr);
    Path filePath = new Path(fileStr);
    FSDataOutputStream stm = dfs.create(filePath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(fileStr));

    // write bytes into the file.
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);
    
    String originalLeaseHolder = NameNodeAdapter.getLeaseHolderForPath(
        cluster.getNameNode(), fileStr);
    
    assertFalse("original lease holder should not be the NN",
        originalLeaseHolder.startsWith(
        HdfsServerConstants.NAMENODE_LEASE_HOLDER));

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    
    // check visible length
    final HdfsDataInputStream in = (HdfsDataInputStream)dfs.open(filePath);
    Assert.assertEquals(size, in.getVisibleLength());
    in.close();
    
    if (doRename) {
      fileStr += ".renamed";
      Path renamedPath = new Path(fileStr);
      assertTrue(dfs.rename(filePath, renamedPath));
      filePath = renamedPath;
    }
    
    // kill the lease renewal thread
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.getLeaseRenewer().interruptAndJoin();
    
    // Make sure the DNs don't send a heartbeat for a while, so the blocks
    // won't actually get completed during lease recovery.
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    
    // set the hard limit to be 1 second 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // Make sure lease recovery begins.
    final String path = fileStr;
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        String holder =
            NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(), path);
        return holder.startsWith(HdfsServerConstants.NAMENODE_LEASE_HOLDER);
      }
    }, (int)SHORT_LEASE_PERIOD, (int)SHORT_LEASE_PERIOD * 10);

    // Normally, the in-progress edit log would be finalized by
    // FSEditLog#endCurrentLogSegment.  For testing purposes, we
    // disable that here.
    FSEditLog spyLog = spy(cluster.getNameNode().getFSImage().getEditLog());
    doNothing().when(spyLog).endCurrentLogSegment(Mockito.anyBoolean());
    DFSTestUtil.setEditLogForTesting(cluster.getNamesystem(), spyLog);

    cluster.restartNameNode(false);
    
    checkLease(fileStr, size);
    
    // Let the DNs send heartbeats again.
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
    }

    cluster.waitActive();

    // set the hard limit to be 1 second, to initiate lease recovery. 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      Thread.sleep(SHORT_LEASE_PERIOD);
      locatedBlocks = dfs.dfs.getLocatedBlocks(fileStr, 0L, size);
    } while (locatedBlocks.isUnderConstruction());
    assertEquals(size, locatedBlocks.getFileLength());

    // make sure that the client can't write data anymore.
    try {
      stm.write('b');
      stm.hflush();
      fail("Should not be able to flush after we've lost the lease");
    } catch (IOException e) {
      LOG.info("Expceted exception on write/hflush", e);
    }
    
    try {
      stm.close();
      fail("Should not be able to close after we've lost the lease");
    } catch (IOException e) {
      LOG.info("Expected exception on close", e);
    }

    // verify data
    AppendTestUtil.LOG.info(
        "File size is good. Now validating sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filePath, size, buffer, fileStr);
  }
  
  static void checkLease(String f, int size) {
    final String holder = NameNodeAdapter.getLeaseHolderForPath(
        cluster.getNameNode(), f); 
    if (size == 0) {
      assertEquals("lease holder should null, file is closed", null, holder);
    } else {
      assertTrue("lease holder should now be the NN",
          holder.startsWith(HdfsServerConstants.NAMENODE_LEASE_HOLDER));
    }
    
  }
}
