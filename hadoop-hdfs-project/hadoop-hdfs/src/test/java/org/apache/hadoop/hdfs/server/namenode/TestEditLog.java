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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLog {
  
  static {
    ((Log4JLogger)FSEditLog.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * A garbage mkdir op which is used for testing
   * {@link EditLogFileInputStream#scanEditLog(File)}
   */
  public static class GarbageMkdirOp extends FSEditLogOp {
    public GarbageMkdirOp() {
      super(FSEditLogOpCodes.OP_MKDIR);
    }

    @Override
    void resetSubFields() {
      // nop
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      throw new IOException("cannot decode GarbageMkdirOp");
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      // write in some garbage content
      Random random = new Random();
      byte[] content = new byte[random.nextInt(16) + 1];
      random.nextBytes(content);
      out.write(content);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      throw new UnsupportedOperationException(
          "Not supported for GarbageMkdirOp");
    }
    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      throw new UnsupportedOperationException(
          "Not supported for GarbageMkdirOp");
    }
  }

  static final Log LOG = LogFactory.getLog(TestEditLog.class);
  
  static final int NUM_DATA_NODES = 0;

  // This test creates NUM_THREADS threads and each thread does
  // 2 * NUM_TRANSACTIONS Transactions concurrently.
  static final int NUM_TRANSACTIONS = 100;
  static final int NUM_THREADS = 100;
  
  static final File TEST_DIR = PathUtils.getTestDir(TestEditLog.class);
  
  /** An edits log with 3 edits from 0.20 - the result of
   * a fresh namesystem followed by hadoop fs -touchz /myfile */
  static final byte[] HADOOP20_SOME_EDITS =
    StringUtils.hexStringToByte((
        "ffff ffed 0a00 0000 0000 03fa e100 0000" +
        "0005 0007 2f6d 7966 696c 6500 0133 000d" +
        "3132 3932 3331 3634 3034 3138 3400 0d31" +
        "3239 3233 3136 3430 3431 3834 0009 3133" +
        "3432 3137 3732 3800 0000 0004 746f 6464" +
        "0a73 7570 6572 6772 6f75 7001 a400 1544" +
        "4653 436c 6965 6e74 5f2d 3136 3136 3535" +
        "3738 3931 000b 3137 322e 3239 2e35 2e33" +
        "3209 0000 0005 0007 2f6d 7966 696c 6500" +
        "0133 000d 3132 3932 3331 3634 3034 3138" +
        "3400 0d31 3239 3233 3136 3430 3431 3834" +
        "0009 3133 3432 3137 3732 3800 0000 0004" +
        "746f 6464 0a73 7570 6572 6772 6f75 7001" +
        "a4ff 0000 0000 0000 0000 0000 0000 0000"
    ).replace(" ",""));

  static {
    // No need to fsync for the purposes of tests. This makes
    // the tests run much faster.
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }
  
  static final byte TRAILER_BYTE = FSEditLogOpCodes.OP_INVALID.getOpCode();

  private static final int CHECKPOINT_ON_STARTUP_MIN_TXNS = 100;
  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    final FSNamesystem namesystem;
    final int numTransactions;
    final short replication = 3;
    final long blockSize = 64;
    final int startIndex;

    Transactions(FSNamesystem ns, int numTx, int startIdx) {
      namesystem = ns;
      numTransactions = numTx;
      startIndex = startIdx;
    }

    // add a bunch of transactions.
    @Override
    public void run() {
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      FSEditLog editLog = namesystem.getEditLog();

      for (int i = 0; i < numTransactions; i++) {
        INodeFile inode = new INodeFile(namesystem.dir.allocateNewInodeId(), null,
            p, 0L, 0L, BlockInfoContiguous.EMPTY_ARRAY, replication, blockSize);
        inode.toUnderConstruction("", "");

        editLog.logOpenFile("/filename" + (startIndex + i), inode, false, false);
        editLog.logCloseFile("/filename" + (startIndex + i), inode);
        editLog.logSync();
      }
    }
  }
  
  /**
   * Construct FSEditLog with default configuration, taking editDirs from NNStorage
   * 
   * @param storage Storage object used by namenode
   */
  private static FSEditLog getFSEditLog(NNStorage storage) throws IOException {
    Configuration conf = new Configuration();
    // Make sure the edits dirs are set in the provided configuration object.
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        StringUtils.join(",", storage.getEditsDirectories()));
    FSEditLog log = new FSEditLog(conf, storage, FSNamesystem.getNamespaceEditsDirs(conf));
    return log;
  }

  /**
   * Test case for an empty edit log from a prior version of Hadoop.
   */
  @Test
  public void testPreTxIdEditLogNoEdits() throws Exception {
    FSNamesystem namesys = Mockito.mock(FSNamesystem.class);
    namesys.dir = Mockito.mock(FSDirectory.class);
    long numEdits = testLoad(
        StringUtils.hexStringToByte("ffffffed"), // just version number
        namesys);
    assertEquals(0, numEdits);
  }
  
  /**
   * Test case for loading a very simple edit log from a format
   * prior to the inclusion of edit transaction IDs in the log.
   */
  @Test
  public void testPreTxidEditLogWithEdits() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();

      long numEdits = testLoad(HADOOP20_SOME_EDITS, namesystem);
      assertEquals(3, numEdits);
      // Sanity check the edit
      HdfsFileStatus fileInfo = namesystem.getFileInfo("/myfile", false);
      assertEquals("supergroup", fileInfo.getGroup());
      assertEquals(3, fileInfo.getReplication());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  private long testLoad(byte[] data, FSNamesystem namesys) throws IOException {
    FSEditLogLoader loader = new FSEditLogLoader(namesys, 0);
    return loader.loadFSEdits(new EditLogByteInputStream(data), 1);
  }

  /**
   * Simple test for writing to and rolling the edit log.
   */
  @Test
  public void testSimpleEditLog() throws IOException {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();
      
      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS, 
          NNStorage.getInProgressEditsFileName(1));
      

      editLog.logSetReplication("fakefile", (short) 1);
      editLog.logSync();
      
      editLog.rollEditLog();

      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS,
          NNStorage.getFinalizedEditsFileName(1,3));
      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS,
          NNStorage.getInProgressEditsFileName(4));

      
      editLog.logSetReplication("fakefile", (short) 2);
      editLog.logSync();
      
      editLog.close();
    } finally {
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
  @Test
  public void testMultiThreadedEditLog() throws IOException {
    testEditLog(2048);
    // force edit buffer to automatically sync on each log of edit log entry
    testEditLog(1);
  }
  
  
  private void assertExistsInStorageDirs(MiniDFSCluster cluster,
      NameNodeDirType dirType,
      String filename) {
    NNStorage storage = cluster.getNamesystem().getFSImage().getStorage();
    for (StorageDirectory sd : storage.dirIterable(dirType)) {
      File f = new File(sd.getCurrentDir(), filename);
      assertTrue("Expect that " + f + " exists", f.exists());
    }
  }
  
  /**
   * Test edit log with different initial buffer size
   * 
   * @param initialSize initial edit log buffer size
   * @throws IOException
   */
  private void testEditLog(int initialSize) throws IOException {

    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
  
      for (Iterator<URI> it = cluster.getNameDirs(0).iterator(); it.hasNext(); ) {
        File dir = new File(it.next().getPath());
        System.out.println(dir);
      }
  
      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();
  
      // set small size of flush buffer
      editLog.setOutputBufferCapacity(initialSize);
      
      // Roll log so new output buffer size takes effect
      // we should now be writing to edits_inprogress_3
      fsimage.rollEditLog();
    
      // Remember the current lastInodeId and will reset it back to test
      // loading editlog segments.The transactions in the following allocate new
      // inode id to write to editlogs but doesn't create ionde in namespace
      long originalLastInodeId = namesystem.dir.getLastInodeId();
      
      // Create threads and make them run transactions concurrently.
      Thread threadId[] = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        Transactions trans =
          new Transactions(namesystem, NUM_TRANSACTIONS, i*NUM_TRANSACTIONS);
        threadId[i] = new Thread(trans, "TransactionThread-" + i);
        threadId[i].start();
      }
  
      // wait for all transactions to get over
      for (int i = 0; i < NUM_THREADS; i++) {
        try {
          threadId[i].join();
        } catch (InterruptedException e) {
          i--;      // retry 
        }
      } 

      // Reopen some files as for append
      Transactions trans = 
        new Transactions(namesystem, NUM_TRANSACTIONS, NUM_TRANSACTIONS / 2);
      trans.run();

      // Roll another time to finalize edits_inprogress_3
      fsimage.rollEditLog();
      
      long expectedTxns = ((NUM_THREADS+1) * 2 * NUM_TRANSACTIONS) + 2; // +2 for start/end txns
   
      // Verify that we can read in all the transactions that we have written.
      // If there were any corruptions, it is likely that the reading in
      // of these transactions will throw an exception.
      //
      namesystem.dir.resetLastInodeIdWithoutChecking(originalLastInodeId);
      for (Iterator<StorageDirectory> it = 
              fsimage.getStorage().dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
        FSEditLogLoader loader = new FSEditLogLoader(namesystem, 0);
        
        File editFile = NNStorage.getFinalizedEditsFile(it.next(), 3,
            3 + expectedTxns - 1);
        assertTrue("Expect " + editFile + " exists", editFile.exists());
        
        System.out.println("Verifying file: " + editFile);
        long numEdits = loader.loadFSEdits(
            new EditLogFileInputStream(editFile), 3);
        int numLeases = namesystem.leaseManager.countLease();
        System.out.println("Number of outstanding leases " + numLeases);
        assertEquals(0, numLeases);
        assertTrue("Verification for " + editFile + " failed. " +
                   "Expected " + expectedTxns + " transactions. "+
                   "Found " + numEdits + " transactions.",
                   numEdits == expectedTxns);
  
      }
    } finally {
      try {
        if(fileSys != null) fileSys.close();
        if(cluster != null) cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Couldn't shut down cleanly", t);
      }
    }
  }

  private void doLogEdit(ExecutorService exec, final FSEditLog log,
    final String filename) throws Exception
  {
    exec.submit(new Callable<Void>() {
      @Override
      public Void call() {
        log.logSetReplication(filename, (short)1);
        return null;
      }
    }).get();
  }
  
  private void doCallLogSync(ExecutorService exec, final FSEditLog log)
    throws Exception
  {
    exec.submit(new Callable<Void>() {
      @Override
      public Void call() {
        log.logSync();
        return null;
      }
    }).get();
  }

  private void doCallLogSyncAll(ExecutorService exec, final FSEditLog log)
    throws Exception
  {
    exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        log.logSyncAll();
        return null;
      }
    }).get();
  }

  @Test
  public void testSyncBatching() throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    ExecutorService threadA = Executors.newSingleThreadExecutor();
    ExecutorService threadB = Executors.newSingleThreadExecutor();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();

      assertEquals("should start with only the BEGIN_LOG_SEGMENT txn synced",
        1, editLog.getSyncTxId());
      
      // Log an edit from thread A
      doLogEdit(threadA, editLog, "thread-a 1");
      assertEquals("logging edit without syncing should do not affect txid",
        1, editLog.getSyncTxId());

      // Log an edit from thread B
      doLogEdit(threadB, editLog, "thread-b 1");
      assertEquals("logging edit without syncing should do not affect txid",
        1, editLog.getSyncTxId());

      // Now ask to sync edit from B, which should sync both edits.
      doCallLogSync(threadB, editLog);
      assertEquals("logSync from second thread should bump txid up to 3",
        3, editLog.getSyncTxId());

      // Now ask to sync edit from A, which was already batched in - thus
      // it should increment the batch count metric
      doCallLogSync(threadA, editLog);
      assertEquals("logSync from first thread shouldn't change txid",
        3, editLog.getSyncTxId());

      //Should have incremented the batch count exactly once
      assertCounter("TransactionsBatchedInSync", 1L, 
        getMetrics("NameNodeActivity"));
    } finally {
      threadA.shutdown();
      threadB.shutdown();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
  
  /**
   * Test what happens with the following sequence:
   *
   *  Thread A writes edit
   *  Thread B calls logSyncAll
   *           calls close() on stream
   *  Thread A calls logSync
   *
   * This sequence is legal and can occur if enterSafeMode() is closely
   * followed by saveNamespace.
   */
  @Test
  public void testBatchedSyncWithClosedLogs() throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    ExecutorService threadA = Executors.newSingleThreadExecutor();
    ExecutorService threadB = Executors.newSingleThreadExecutor();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();

      // Log an edit from thread A
      doLogEdit(threadA, editLog, "thread-a 1");
      assertEquals("logging edit without syncing should do not affect txid",
        1, editLog.getSyncTxId());

      // logSyncAll in Thread B
      doCallLogSyncAll(threadB, editLog);
      assertEquals("logSyncAll should sync thread A's transaction",
        2, editLog.getSyncTxId());

      // Close edit log
      editLog.close();

      // Ask thread A to finish sync (which should be a no-op)
      doCallLogSync(threadA, editLog);
    } finally {
      threadA.shutdown();
      threadB.shutdown();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
  
  @Test
  public void testEditChecksum() throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    final FSEditLog editLog = fsimage.getEditLog();
    fileSys.mkdirs(new Path("/tmp"));

    Iterator<StorageDirectory> iter = fsimage.getStorage().
      dirIterator(NameNodeDirType.EDITS);
    LinkedList<StorageDirectory> sds = new LinkedList<StorageDirectory>();
    while (iter.hasNext()) {
      sds.add(iter.next());
    }
    editLog.close();
    cluster.shutdown();

    for (StorageDirectory sd : sds) {
      File editFile = NNStorage.getFinalizedEditsFile(sd, 1, 3);
      assertTrue(editFile.exists());
  
      long fileLen = editFile.length();
      LOG.debug("Corrupting Log File: " + editFile + " len: " + fileLen);
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(fileLen-4); // seek to checksum bytes
      int b = rwf.readInt();
      rwf.seek(fileLen-4);
      rwf.writeInt(b+1);
      rwf.close();
    }
    
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).format(false).build();
      fail("should not be able to start");
    } catch (IOException e) {
      // expected
      assertNotNull("Cause of exception should be ChecksumException", e.getCause());
      assertEquals("Cause of exception should be ChecksumException",
          ChecksumException.class, e.getCause().getClass());
    }
  }

  /**
   * Test what happens if the NN crashes when it has has started but
   * had no transactions written.
   */
  @Test
  public void testCrashRecoveryNoTransactions() throws Exception {
    testCrashRecovery(0);
  }
  
  /**
   * Test what happens if the NN crashes when it has has started and
   * had a few transactions written
   */
  @Test
  public void testCrashRecoveryWithTransactions() throws Exception {
    testCrashRecovery(150);
  }
  
  /**
   * Do a test to make sure the edit log can recover edits even after
   * a non-clean shutdown. This does a simulated crash by copying over
   * the edits directory while the NN is still running, then shutting it
   * down, and restoring that edits directory.
   */
  private void testCrashRecovery(int numTransactions) throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
        CHECKPOINT_ON_STARTUP_MIN_TXNS);
    
    try {
        LOG.info("\n===========================================\n" +
                 "Starting empty cluster");
        
        cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(NUM_DATA_NODES)
          .format(true)
          .build();
        cluster.waitActive();
        
        FileSystem fs = cluster.getFileSystem();
        for (int i = 0; i < numTransactions; i++) {
          fs.mkdirs(new Path("/test" + i));
        }        
        
        // Directory layout looks like:
        // test/data/dfs/nameN/current/{fsimage_N,edits_...}
        File nameDir = new File(cluster.getNameDirs(0).iterator().next().getPath());
        File dfsDir = nameDir.getParentFile();
        assertEquals(dfsDir.getName(), "dfs"); // make sure we got right dir
        
        LOG.info("Copying data directory aside to a hot backup");
        File backupDir = new File(dfsDir.getParentFile(), "dfs.backup-while-running");
        FileUtils.copyDirectory(dfsDir, backupDir);

        LOG.info("Shutting down cluster #1");
        cluster.shutdown();
        cluster = null;
        
        // Now restore the backup
        FileUtil.fullyDeleteContents(dfsDir);
        dfsDir.delete();
        backupDir.renameTo(dfsDir);
        
        // Directory layout looks like:
        // test/data/dfs/nameN/current/{fsimage_N,edits_...}
        File currentDir = new File(nameDir, "current");

        // We should see the file as in-progress
        File editsFile = new File(currentDir,
            NNStorage.getInProgressEditsFileName(1));
        assertTrue("Edits file " + editsFile + " should exist", editsFile.exists());        
        
        File imageFile = FSImageTestUtil.findNewestImageFile(
            currentDir.getAbsolutePath());
        assertNotNull("No image found in " + nameDir, imageFile);
        assertEquals(NNStorage.getImageFileName(0), imageFile.getName());
        // Try to start a new cluster
        LOG.info("\n===========================================\n" +
        "Starting same cluster after simulated crash");
        cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(NUM_DATA_NODES)
          .format(false)
          .build();
        cluster.waitActive();
        
        // We should still have the files we wrote prior to the simulated crash
        fs = cluster.getFileSystem();
        for (int i = 0; i < numTransactions; i++) {
          assertTrue(fs.exists(new Path("/test" + i)));
        }

        long expectedTxId;
        if (numTransactions > CHECKPOINT_ON_STARTUP_MIN_TXNS) {
          // It should have saved a checkpoint on startup since there
          // were more unfinalized edits than configured
          expectedTxId = numTransactions + 1;
        } else {
          // otherwise, it shouldn't have made a checkpoint
          expectedTxId = 0;
        }
        imageFile = FSImageTestUtil.findNewestImageFile(
            currentDir.getAbsolutePath());
        assertNotNull("No image found in " + nameDir, imageFile);
        assertEquals(NNStorage.getImageFileName(expectedTxId),
                     imageFile.getName());
        
        // Started successfully. Shut it down and make sure it can restart.
        cluster.shutdown();    
        cluster = null;
        
        cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATA_NODES)
        .format(false)
        .build();
        cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  // should succeed - only one corrupt log dir
  @Test
  public void testCrashRecoveryEmptyLogOneDir() throws Exception {
    doTestCrashRecoveryEmptyLog(false, true, true);
  }
  
  // should fail - seen_txid updated to 3, but no log dir contains txid 3
  @Test
  public void testCrashRecoveryEmptyLogBothDirs() throws Exception {
    doTestCrashRecoveryEmptyLog(true, true, false);
  }

  // should succeed - only one corrupt log dir
  @Test
  public void testCrashRecoveryEmptyLogOneDirNoUpdateSeenTxId() 
      throws Exception {
    doTestCrashRecoveryEmptyLog(false, false, true);
  }
  
  // should succeed - both log dirs corrupt, but seen_txid never updated
  @Test
  public void testCrashRecoveryEmptyLogBothDirsNoUpdateSeenTxId()
      throws Exception {
    doTestCrashRecoveryEmptyLog(true, false, true);
  }

  /**
   * Test that the NN handles the corruption properly
   * after it crashes just after creating an edit log
   * (ie before writing START_LOG_SEGMENT). In the case
   * that all logs have this problem, it should mark them
   * as corrupt instead of trying to finalize them.
   * 
   * @param inBothDirs if true, there will be a truncated log in
   * both of the edits directories. If false, the truncated log
   * will only be in one of the directories. In both cases, the
   * NN should fail to start up, because it's aware that txid 3
   * was reached, but unable to find a non-corrupt log starting there.
   * @param updateTransactionIdFile if true update the seen_txid file.
   * If false, it will not be updated. This will simulate a case where
   * the NN crashed between creating the new segment and updating the
   * seen_txid file.
   * @param shouldSucceed true if the test is expected to succeed.
   */
  private void doTestCrashRecoveryEmptyLog(boolean inBothDirs, 
      boolean updateTransactionIdFile, boolean shouldSucceed)
      throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(NUM_DATA_NODES).build();
    cluster.shutdown();
    
    Collection<URI> editsDirs = cluster.getNameEditsDirs(0);
    for (URI uri : editsDirs) {
      File dir = new File(uri.getPath());
      File currentDir = new File(dir, "current");
      // We should start with only the finalized edits_1-2
      GenericTestUtils.assertGlobEquals(currentDir, "edits_.*",
          NNStorage.getFinalizedEditsFileName(1, 2));
      // Make a truncated edits_3_inprogress
      File log = new File(currentDir,
          NNStorage.getInProgressEditsFileName(3));

      EditLogFileOutputStream stream = new EditLogFileOutputStream(conf, log, 1024);
      try {
        stream.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
        if (!inBothDirs) {
          break;
        }
        
        NNStorage storage = new NNStorage(conf, 
            Collections.<URI>emptyList(),
            Lists.newArrayList(uri));
        
        if (updateTransactionIdFile) {
          storage.writeTransactionIdFileToStorage(3);
        }
        storage.close();
      } finally {
        stream.close();
      }
    }
    
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATA_NODES).format(false).build();
      if (!shouldSucceed) {
        fail("Should not have succeeded in startin cluster");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        LOG.info("Should have succeeded in starting cluster, but failed", ioe);
        throw ioe;
      } else {
        GenericTestUtils.assertExceptionContains(
          "Gap in transactions. Expected to be able to read up until " +
          "at least txid 3 but unable to find any edit logs containing " +
          "txid 3", ioe);
      }
    } finally {
      cluster.shutdown();
    }
  }

  
  private static class EditLogByteInputStream extends EditLogInputStream {
    private final InputStream input;
    private final long len;
    private int version;
    private FSEditLogOp.Reader reader = null;
    private FSEditLogLoader.PositionTrackingInputStream tracker = null;

    public EditLogByteInputStream(byte[] data) throws IOException {
      len = data.length;
      input = new ByteArrayInputStream(data);

      BufferedInputStream bin = new BufferedInputStream(input);
      DataInputStream in = new DataInputStream(bin);
      version = EditLogFileInputStream.readLogVersion(in, true);
      tracker = new FSEditLogLoader.PositionTrackingInputStream(in);
      in = new DataInputStream(tracker);
            
      reader = new FSEditLogOp.Reader(in, tracker, version);
    }
  
    @Override
    public long getFirstTxId() {
      return HdfsConstants.INVALID_TXID;
    }
    
    @Override
    public long getLastTxId() {
      return HdfsConstants.INVALID_TXID;
    }
  
    @Override
    public long length() throws IOException {
      return len;
    }
  
    @Override
    public long getPosition() {
      return tracker.getPos();
    }

    @Override
    protected FSEditLogOp nextOp() throws IOException {
      return reader.readOp(false);
    }

    @Override
    public int getVersion(boolean verifyVersion) throws IOException {
      return version;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public String getName() {
      return "AnonEditLogByteInputStream";
    }

    @Override
    public boolean isInProgress() {
      return true;
    }

    @Override
    public void setMaxOpSize(int maxOpSize) {
      reader.setMaxOpSize(maxOpSize);
    }

    @Override public boolean isLocalLog() {
      return true;
    }
  }

  @Test
  public void testFailedOpen() throws Exception {
    File logDir = new File(TEST_DIR, "testFailedOpen");
    logDir.mkdirs();
    FSEditLog log = FSImageTestUtil.createStandaloneEditLog(logDir);
    try {
      FileUtil.setWritable(logDir, false);
      log.openForWrite();
      fail("Did no throw exception on only having a bad dir");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "too few journals successfully started", ioe);
    } finally {
      FileUtil.setWritable(logDir, true);
      log.close();
    }
  }
  
  /**
   * Regression test for HDFS-1112/HDFS-3020. Ensures that, even if
   * logSync isn't called periodically, the edit log will sync itself.
   */
  @Test
  public void testAutoSync() throws Exception {
    File logDir = new File(TEST_DIR, "testAutoSync");
    logDir.mkdirs();
    FSEditLog log = FSImageTestUtil.createStandaloneEditLog(logDir);
    
    String oneKB = StringUtils.byteToHexString(
        new byte[500]);
    
    try {
      log.openForWrite();
      NameNodeMetrics mockMetrics = Mockito.mock(NameNodeMetrics.class);
      log.setMetricsForTests(mockMetrics);

      for (int i = 0; i < 400; i++) {
        log.logDelete(oneKB, 1L, false);
      }
      // After ~400KB, we're still within the 512KB buffer size
      Mockito.verify(mockMetrics, Mockito.times(0)).addSync(Mockito.anyLong());
      
      // After ~400KB more, we should have done an automatic sync
      for (int i = 0; i < 400; i++) {
        log.logDelete(oneKB, 1L, false);
      }
      Mockito.verify(mockMetrics, Mockito.times(1)).addSync(Mockito.anyLong());

    } finally {
      log.close();
    }
  }

  /**
   * Tests the getEditLogManifest function using mock storage for a number
   * of different situations.
   */
  @Test
  public void testEditLogManifestMocks() throws IOException {
    NNStorage storage;
    FSEditLog log;
    // Simple case - different directories have the same
    // set of logs, with an in-progress one at end
    storage = mockStorageWithEdits(
        "[1,100]|[101,200]|[201,]",
        "[1,100]|[101,200]|[201,]");
    log = getFSEditLog(storage);
    log.initJournalsForWrite();
    assertEquals("[[1,100], [101,200]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200]]",
        log.getEditLogManifest(101).toString());

    // Another simple case, different directories have different
    // sets of files
    storage = mockStorageWithEdits(
        "[1,100]|[101,200]",
        "[1,100]|[201,300]|[301,400]"); // nothing starting at 101
    log = getFSEditLog(storage);
    log.initJournalsForWrite();
    assertEquals("[[1,100], [101,200], [201,300], [301,400]]",
        log.getEditLogManifest(1).toString());
    
    // Case where one directory has an earlier finalized log, followed
    // by a gap. The returned manifest should start after the gap.
    storage = mockStorageWithEdits(
        "[1,100]|[301,400]", // gap from 101 to 300
        "[301,400]|[401,500]");
    log = getFSEditLog(storage);
    log.initJournalsForWrite();
    assertEquals("[[301,400], [401,500]]",
        log.getEditLogManifest(1).toString());
    
    // Case where different directories have different length logs
    // starting at the same txid - should pick the longer one
    storage = mockStorageWithEdits(
        "[1,100]|[101,150]", // short log at 101
        "[1,50]|[101,200]"); // short log at 1
    log = getFSEditLog(storage);
    log.initJournalsForWrite();
    assertEquals("[[1,100], [101,200]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200]]",
        log.getEditLogManifest(101).toString());

    // Case where the first storage has an inprogress while
    // the second has finalised that file (i.e. the first failed
    // recently)
    storage = mockStorageWithEdits(
        "[1,100]|[101,]", 
        "[1,100]|[101,200]"); 
    log = getFSEditLog(storage);
    log.initJournalsForWrite();
    assertEquals("[[1,100], [101,200]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200]]",
        log.getEditLogManifest(101).toString());
  }
  
  /**
   * Create a mock NNStorage object with several directories, each directory
   * holding edit logs according to a specification. Each directory
   * is specified by a pipe-separated string. For example:
   * <code>[1,100]|[101,200]</code> specifies a directory which
   * includes two finalized segments, one from 1-100, and one from 101-200.
   * The syntax <code>[1,]</code> specifies an in-progress log starting at
   * txid 1.
   */
  private NNStorage mockStorageWithEdits(String... editsDirSpecs) throws IOException {
    List<StorageDirectory> sds = Lists.newArrayList();
    List<URI> uris = Lists.newArrayList();

    NNStorage storage = Mockito.mock(NNStorage.class);
    for (String dirSpec : editsDirSpecs) {
      List<String> files = Lists.newArrayList();
      String[] logSpecs = dirSpec.split("\\|");
      for (String logSpec : logSpecs) {
        Matcher m = Pattern.compile("\\[(\\d+),(\\d+)?\\]").matcher(logSpec);
        assertTrue("bad spec: " + logSpec, m.matches());
        if (m.group(2) == null) {
          files.add(NNStorage.getInProgressEditsFileName(
              Long.parseLong(m.group(1))));
        } else {
          files.add(NNStorage.getFinalizedEditsFileName(
              Long.parseLong(m.group(1)),
              Long.parseLong(m.group(2))));
        }
      }
      StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
          NameNodeDirType.EDITS, false,
          files.toArray(new String[0]));
      sds.add(sd);
      URI u = URI.create("file:///storage"+ Math.random());
      Mockito.doReturn(sd).when(storage).getStorageDirectory(u);
      uris.add(u);
    }    

    Mockito.doReturn(sds).when(storage).dirIterable(NameNodeDirType.EDITS);
    Mockito.doReturn(uris).when(storage).getEditsDirectories();
    return storage;
  }

  /** 
   * Specification for a failure during #setupEdits
   */
  static class AbortSpec {
    final int roll;
    final int logindex;
    
    /**
     * Construct the failure specification. 
     * @param roll number to fail after. e.g. 1 to fail after the first roll
     * @param loginfo index of journal to fail. 
     */
    AbortSpec(int roll, int logindex) {
      this.roll = roll;
      this.logindex = logindex;
    }
  }

  final static int TXNS_PER_ROLL = 10;  
  final static int TXNS_PER_FAIL = 2;
    
  /**
   * Set up directories for tests. 
   *
   * Each rolled file is 10 txns long. 
   * A failed file is 2 txns long.
   * 
   * @param editUris directories to create edit logs in
   * @param numrolls number of times to roll the edit log during setup
   * @param closeOnFinish whether to close the edit log after setup
   * @param abortAtRolls Specifications for when to fail, see AbortSpec
   */
  public static NNStorage setupEdits(List<URI> editUris, int numrolls,
      boolean closeOnFinish, AbortSpec... abortAtRolls) throws IOException {
    List<AbortSpec> aborts = new ArrayList<AbortSpec>(Arrays.asList(abortAtRolls));
    NNStorage storage = new NNStorage(new Configuration(),
                                      Collections.<URI>emptyList(),
                                      editUris);
    storage.format(new NamespaceInfo());
    FSEditLog editlog = getFSEditLog(storage);    
    // open the edit log and add two transactions
    // logGenerationStamp is used, simply because it doesn't 
    // require complex arguments.
    editlog.initJournalsForWrite();
    editlog.openForWrite();
    for (int i = 2; i < TXNS_PER_ROLL; i++) {
      editlog.logGenerationStampV2((long) 0);
    }
    editlog.logSync();
    
    // Go into edit log rolling loop.
    // On each roll, the abortAtRolls abort specs are 
    // checked to see if an abort is required. If so the 
    // the specified journal is aborted. It will be brought
    // back into rotation automatically by rollEditLog
    for (int i = 0; i < numrolls; i++) {
      editlog.rollEditLog();
      
      editlog.logGenerationStampV2((long) i);
      editlog.logSync();

      while (aborts.size() > 0 
             && aborts.get(0).roll == (i+1)) {
        AbortSpec spec = aborts.remove(0);
        editlog.getJournals().get(spec.logindex).abort();
      } 
      
      for (int j = 3; j < TXNS_PER_ROLL; j++) {
        editlog.logGenerationStampV2((long) i);
      }
      editlog.logSync();
    }
    
    if (closeOnFinish) {
      editlog.close();
    }

    FSImageTestUtil.logStorageContents(LOG, storage);
    return storage;
  }
    
  /**
   * Set up directories for tests. 
   *
   * Each rolled file is 10 txns long. 
   * A failed file is 2 txns long.
   * 
   * @param editUris directories to create edit logs in
   * @param numrolls number of times to roll the edit log during setup
   * @param abortAtRolls Specifications for when to fail, see AbortSpec
   */
  public static NNStorage setupEdits(List<URI> editUris, int numrolls, 
      AbortSpec... abortAtRolls) throws IOException {
    return setupEdits(editUris, numrolls, true, abortAtRolls);
  }

  /** 
   * Test loading an editlog which has had both its storage fail
   * on alternating rolls. Two edit log directories are created.
   * The first one fails on odd rolls, the second on even. Test
   * that we are able to load the entire editlog regardless.
   */
  @Test
  public void testAlternatingJournalFailure() throws IOException {
    File f1 = new File(TEST_DIR + "/alternatingjournaltest0");
    File f2 = new File(TEST_DIR + "/alternatingjournaltest1");

    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI());
    
    NNStorage storage = setupEdits(editUris, 10,
                                   new AbortSpec(1, 0),
                                   new AbortSpec(2, 1),
                                   new AbortSpec(3, 0),
                                   new AbortSpec(4, 1),
                                   new AbortSpec(5, 0),
                                   new AbortSpec(6, 1),
                                   new AbortSpec(7, 0),
                                   new AbortSpec(8, 1),
                                   new AbortSpec(9, 0),
                                   new AbortSpec(10, 1));
    long totaltxnread = 0;
    FSEditLog editlog = getFSEditLog(storage);
    editlog.initJournalsForWrite();
    long startTxId = 1;
    Iterable<EditLogInputStream> editStreams = editlog.selectInputStreams(startTxId, 
                                                                          TXNS_PER_ROLL*11);

    for (EditLogInputStream edits : editStreams) {
      FSEditLogLoader.EditLogValidation val = FSEditLogLoader.validateEditLog(edits);
      long read = (val.getEndTxId() - edits.getFirstTxId()) + 1;
      LOG.info("Loading edits " + edits + " read " + read);
      assertEquals(startTxId, edits.getFirstTxId());
      startTxId += read;
      totaltxnread += read;
    }

    editlog.close();
    storage.close();
    assertEquals(TXNS_PER_ROLL*11, totaltxnread);    
  }

  /** 
   * Test loading an editlog with gaps. A single editlog directory
   * is set up. On of the edit log files is deleted. This should
   * fail when selecting the input streams as it will not be able 
   * to select enough streams to load up to 4*TXNS_PER_ROLL.
   * There should be 4*TXNS_PER_ROLL transactions as we rolled 3
   * times. 
   */
  @Test
  public void testLoadingWithGaps() throws IOException {
    File f1 = new File(TEST_DIR + "/gaptest0");
    List<URI> editUris = ImmutableList.of(f1.toURI());

    NNStorage storage = setupEdits(editUris, 3);
    
    final long startGapTxId = 1*TXNS_PER_ROLL + 1;
    final long endGapTxId = 2*TXNS_PER_ROLL;

    File[] files = new File(f1, "current").listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startGapTxId, 
                                  endGapTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());
    
    FSEditLog editlog = getFSEditLog(storage);
    editlog.initJournalsForWrite();
    long startTxId = 1;
    try {
      editlog.selectInputStreams(startTxId, 4*TXNS_PER_ROLL);
      fail("Should have thrown exception");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Gap in transactions. Expected to be able to read up until " +
          "at least txid 40 but unable to find any edit logs containing " +
          "txid 11", ioe);
    }
  }

  /**
   * Test that we can read from a byte stream without crashing.
   *
   */
  static void validateNoCrash(byte garbage[]) throws IOException {
    final File TEST_LOG_NAME = new File(TEST_DIR, "test_edit_log");

    EditLogFileOutputStream elfos = null;
    EditLogFileInputStream elfis = null;
    try {
      elfos = new EditLogFileOutputStream(new Configuration(), TEST_LOG_NAME, 0);
      elfos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      elfos.writeRaw(garbage, 0, garbage.length);
      elfos.setReadyToFlush();
      elfos.flushAndSync(true);
      elfos.close();
      elfos = null;
      elfis = new EditLogFileInputStream(TEST_LOG_NAME);

      // verify that we can read everything without killing the JVM or
      // throwing an exception other than IOException
      try {
        while (true) {
          FSEditLogOp op = elfis.readOp();
          if (op == null)
            break;
        }
      } catch (IOException e) {
      } catch (Throwable t) {
        fail("Caught non-IOException throwable " +
             StringUtils.stringifyException(t));
      }
    } finally {
      if ((elfos != null) && (elfos.isOpen()))
        elfos.close();
      if (elfis != null)
        elfis.close();
    }
  }

  static byte[][] invalidSequenecs = null;

  /**
   * "Fuzz" test for the edit log.
   *
   * This tests that we can read random garbage from the edit log without
   * crashing the JVM or throwing an unchecked exception.
   */
  @Test
  public void testFuzzSequences() throws IOException {
    final int MAX_GARBAGE_LENGTH = 512;
    final int MAX_INVALID_SEQ = 5000;
    // The seed to use for our random number generator.  When given the same
    // seed, Java.util.Random will always produce the same sequence of values.
    // This is important because it means that the test is deterministic and
    // repeatable on any machine.
    final int RANDOM_SEED = 123;

    Random r = new Random(RANDOM_SEED);
    for (int i = 0; i < MAX_INVALID_SEQ; i++) {
      byte[] garbage = new byte[r.nextInt(MAX_GARBAGE_LENGTH)];
      r.nextBytes(garbage);
      validateNoCrash(garbage);
    }
  }

  private static long readAllEdits(Collection<EditLogInputStream> streams,
      long startTxId) throws IOException {
    FSEditLogOp op;
    long nextTxId = startTxId;
    long numTx = 0;
    for (EditLogInputStream s : streams) {
      while (true) {
        op = s.readOp();
        if (op == null)
          break;
        if (op.getTransactionId() != nextTxId) {
          throw new IOException("out of order transaction ID!  expected " +
              nextTxId + " but got " + op.getTransactionId() + " when " +
              "reading " + s.getName());
        }
        numTx++;
        nextTxId = op.getTransactionId() + 1;
      }
    }
    return numTx;
  }

  /**
   * Test edit log failover.  If a single edit log is missing, other 
   * edits logs should be used instead.
   */
  @Test
  public void testEditLogFailOverFromMissing() throws IOException {
    File f1 = new File(TEST_DIR + "/failover0");
    File f2 = new File(TEST_DIR + "/failover1");
    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI());

    NNStorage storage = setupEdits(editUris, 3);
    
    final long startErrorTxId = 1*TXNS_PER_ROLL + 1;
    final long endErrorTxId = 2*TXNS_PER_ROLL;

    File[] files = new File(f1, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startErrorTxId, 
                                  endErrorTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());

    FSEditLog editlog = getFSEditLog(storage);
    editlog.initJournalsForWrite();
    long startTxId = 1;
    Collection<EditLogInputStream> streams = null;
    try {
      streams = editlog.selectInputStreams(startTxId, 4*TXNS_PER_ROLL);
      readAllEdits(streams, startTxId);
    } catch (IOException e) {
      LOG.error("edit log failover didn't work", e);
      fail("Edit log failover didn't work");
    } finally {
      IOUtils.cleanup(null, streams.toArray(new EditLogInputStream[0]));
    }
  }

  /** 
   * Test edit log failover from a corrupt edit log
   */
  @Test
  public void testEditLogFailOverFromCorrupt() throws IOException {
    File f1 = new File(TEST_DIR + "/failover0");
    File f2 = new File(TEST_DIR + "/failover1");
    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI());

    NNStorage storage = setupEdits(editUris, 3);
    
    final long startErrorTxId = 1*TXNS_PER_ROLL + 1;
    final long endErrorTxId = 2*TXNS_PER_ROLL;

    File[] files = new File(f1, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startErrorTxId, 
                                  endErrorTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);

    long fileLen = files[0].length();
    LOG.debug("Corrupting Log File: " + files[0] + " len: " + fileLen);
    RandomAccessFile rwf = new RandomAccessFile(files[0], "rw");
    rwf.seek(fileLen-4); // seek to checksum bytes
    int b = rwf.readInt();
    rwf.seek(fileLen-4);
    rwf.writeInt(b+1);
    rwf.close();
    
    FSEditLog editlog = getFSEditLog(storage);
    editlog.initJournalsForWrite();
    long startTxId = 1;
    Collection<EditLogInputStream> streams = null;
    try {
      streams = editlog.selectInputStreams(startTxId, 4*TXNS_PER_ROLL);
      readAllEdits(streams, startTxId);
    } catch (IOException e) {
      LOG.error("edit log failover didn't work", e);
      fail("Edit log failover didn't work");
    } finally {
      IOUtils.cleanup(null, streams.toArray(new EditLogInputStream[0]));
    }
  }

  /**
   * Test creating a directory with lots and lots of edit log segments
   */
  @Test
  public void testManyEditLogSegments() throws IOException {
    final int NUM_EDIT_LOG_ROLLS = 1000;
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();
      for (int i = 0; i < NUM_EDIT_LOG_ROLLS; i++){
        editLog.logSetReplication("fakefile" + i, (short)(i % 3));
        assertExistsInStorageDirs(
            cluster, NameNodeDirType.EDITS,
            NNStorage.getInProgressEditsFileName((i * 3) + 1));
        editLog.logSync();
        editLog.rollEditLog();
        assertExistsInStorageDirs(
            cluster, NameNodeDirType.EDITS,
            NNStorage.getFinalizedEditsFileName((i * 3) + 1, (i * 3) + 3));
      }
      editLog.close();
    } finally {
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }

    // How long does it take to read through all these edit logs?
    long startTime = Time.now();
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          numDataNodes(NUM_DATA_NODES).build();
      cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    long endTime = Time.now();
    double delta = ((float)(endTime - startTime)) / 1000.0;
    LOG.info(String.format("loaded %d edit log segments in %.2f seconds",
        NUM_EDIT_LOG_ROLLS, delta));
  }

  /**
   * Edit log op instances are cached internally using thread-local storage.
   * This test checks that the cached instances are reset in between different
   * transactions processed on the same thread, so that we don't accidentally
   * apply incorrect attributes to an inode.
   *
   * @throws IOException if there is an I/O error
   */
  @Test
  public void testResetThreadLocalCachedOps() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    // Set single handler thread, so all transactions hit same thread-local ops.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 1);
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();

      // Create /dir1 with a default ACL.
      Path dir1 = new Path("/dir1");
      fileSys.mkdirs(dir1);
      List<AclEntry> aclSpec = Lists.newArrayList(
          aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
      fileSys.modifyAclEntries(dir1, aclSpec);

      // /dir1/dir2 is expected to clone the default ACL.
      Path dir2 = new Path("/dir1/dir2");
      fileSys.mkdirs(dir2);

      // /dir1/file1 is expected to clone the default ACL.
      Path file1 = new Path("/dir1/file1");
      fileSys.create(file1).close();

      // /dir3 is not a child of /dir1, so must not clone the default ACL.
      Path dir3 = new Path("/dir3");
      fileSys.mkdirs(dir3);

      // /file2 is not a child of /dir1, so must not clone the default ACL.
      Path file2 = new Path("/file2");
      fileSys.create(file2).close();

      // Restart and assert the above stated expectations.
      IOUtils.cleanup(LOG, fileSys);
      cluster.restartNameNode();
      fileSys = cluster.getFileSystem();
      assertFalse(fileSys.getAclStatus(dir1).getEntries().isEmpty());
      assertFalse(fileSys.getAclStatus(dir2).getEntries().isEmpty());
      assertFalse(fileSys.getAclStatus(file1).getEntries().isEmpty());
      assertTrue(fileSys.getAclStatus(dir3).getEntries().isEmpty());
      assertTrue(fileSys.getAclStatus(file2).getEntries().isEmpty());
    } finally {
      IOUtils.cleanup(LOG, fileSys);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
