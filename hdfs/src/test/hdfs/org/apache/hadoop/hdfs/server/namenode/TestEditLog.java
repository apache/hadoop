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

import junit.framework.TestCase;
import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.aspectj.util.FileUtil;

import org.mockito.Mockito;

import com.google.common.collect.Lists;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLog extends TestCase {
  
  static {
    ((Log4JLogger)FSEditLog.LOG).getLogger().setLevel(Level.ALL);
  }
  
  static final Log LOG = LogFactory.getLog(TestEditLog.class);
  
  static final int NUM_DATA_NODES = 0;

  // This test creates NUM_THREADS threads and each thread does
  // 2 * NUM_TRANSACTIONS Transactions concurrently.
  static final int NUM_TRANSACTIONS = 100;
  static final int NUM_THREADS = 100;
  
  private static final File TEST_DIR = new File(
    System.getProperty("test.build.data","build/test/data"));

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

  
  static final byte TRAILER_BYTE = FSEditLogOpCodes.OP_INVALID.getOpCode();
  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    FSNamesystem namesystem;
    int numTransactions;
    short replication = 3;
    long blockSize = 64;

    Transactions(FSNamesystem ns, int num) {
      namesystem = ns;
      numTransactions = num;
    }

    // add a bunch of transactions.
    public void run() {
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      FSEditLog editLog = namesystem.getEditLog();

      for (int i = 0; i < numTransactions; i++) {
        INodeFileUnderConstruction inode = new INodeFileUnderConstruction(
                            p, replication, blockSize, 0, "", "", null);
        editLog.logOpenFile("/filename" + i, inode);
        editLog.logCloseFile("/filename" + i, inode);
        editLog.logSync();
      }
    }
  }

  /**
   * Test case for an empty edit log from a prior version of Hadoop.
   */
  public void testPreTxIdEditLogNoEdits() throws Exception {
    FSNamesystem namesys = Mockito.mock(FSNamesystem.class);
    namesys.dir = Mockito.mock(FSDirectory.class);
    int numEdits = testLoad(
        StringUtils.hexStringToByte("ffffffed"), // just version number
        namesys);
    assertEquals(0, numEdits);
  }
  
  /**
   * Test case for loading a very simple edit log from a format
   * prior to the inclusion of edit transaction IDs in the log.
   */
  public void testPreTxidEditLogWithEdits() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();

      int numEdits = testLoad(HADOOP20_SOME_EDITS, namesystem);
      assertEquals(3, numEdits);
      // Sanity check the edit
      HdfsFileStatus fileInfo = namesystem.getFileInfo("/myfile", false);
      assertEquals("supergroup", fileInfo.getGroup());
      assertEquals(3, fileInfo.getReplication());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  private int testLoad(byte[] data, FSNamesystem namesys) throws IOException {
    FSEditLogLoader loader = new FSEditLogLoader(namesys);
    return loader.loadFSEdits(new EditLogByteInputStream(data), 1);
  }

  /**
   * Simple test for writing to and rolling the edit log.
   */
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
      try {
        if(fileSys != null) fileSys.close();
        if(cluster != null) cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Couldn't shut down", t);
      }
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
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
    
      // Create threads and make them run transactions concurrently.
      Thread threadId[] = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        Transactions trans = new Transactions(namesystem, NUM_TRANSACTIONS);
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
      
      // Roll another time to finalize edits_inprogress_3
      fsimage.rollEditLog();
      
      long expectedTxns = (NUM_THREADS * 2 * NUM_TRANSACTIONS) + 2; // +2 for start/end txns
   
      // Verify that we can read in all the transactions that we have written.
      // If there were any corruptions, it is likely that the reading in
      // of these transactions will throw an exception.
      //
      for (Iterator<StorageDirectory> it = 
              fsimage.getStorage().dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
        FSEditLogLoader loader = new FSEditLogLoader(namesystem);
        
        File editFile = NNStorage.getFinalizedEditsFile(it.next(), 3,
            3 + expectedTxns - 1);
        assertTrue("Expect " + editFile + " exists", editFile.exists());
        
        System.out.println("Verifying file: " + editFile);
        int numEdits = loader.loadFSEdits(
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
      public Void call() throws Exception {
        log.logSyncAll();
        return null;
      }
    }).get();
  }

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
      assertEquals("logSync from second thread should bump txid up to 2",
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
    StorageDirectory sd = fsimage.getStorage().dirIterator(NameNodeDirType.EDITS).next();
    editLog.close();
    cluster.shutdown();

    File editFile = NNStorage.getFinalizedEditsFile(sd, 1, 3);
    assertTrue(editFile.exists());

    long fileLen = editFile.length();
    System.out.println("File name: " + editFile + " len: " + fileLen);
    RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
    rwf.seek(fileLen-4); // seek to checksum bytes
    int b = rwf.readInt();
    rwf.seek(fileLen-4);
    rwf.writeInt(b+1);
    rwf.close();
    
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).format(false).build();
      fail("should not be able to start");
    } catch (IOException e) {
      // expected
      assertEquals("Cause of exception should be ChecksumException",
          e.getCause().getClass(), ChecksumException.class);
    }
  }

  /**
   * Test what happens if the NN crashes when it has has started but
   * had no transactions written.
   */
  public void testCrashRecoveryNoTransactions() throws Exception {
    testCrashRecovery(0);
  }
  
  /**
   * Test what happens if the NN crashes when it has has started and
   * had a few transactions written
   */
  public void testCrashRecoveryWithTransactions() throws Exception {
    testCrashRecovery(3);
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
        FileUtil.copyDir(dfsDir, backupDir);;

        LOG.info("Shutting down cluster #1");
        cluster.shutdown();
        cluster = null;
        
        // Now restore the backup
        FileUtil.deleteContents(dfsDir);
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

        // It should have saved a checkpoint on startup since there
        // were unfinalized edits
        long expectedTxId = numTransactions + 1;
        imageFile = FSImageTestUtil.findNewestImageFile(
            currentDir.getAbsolutePath());
        assertNotNull("No image found in " + nameDir, imageFile);
        assertEquals(NNStorage.getImageFileName(expectedTxId),
                     imageFile.getName());
        
        // Started successfully
        cluster.shutdown();    
        cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  public void testCrashRecoveryEmptyLogOneDir() throws Exception {
    doTestCrashRecoveryEmptyLog(false);
  }
  
  public void testCrashRecoveryEmptyLogBothDirs() throws Exception {
    doTestCrashRecoveryEmptyLog(true);
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
   */
  private void doTestCrashRecoveryEmptyLog(boolean inBothDirs) throws Exception {
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
      new EditLogFileOutputStream(log, 1024).create();
      if (!inBothDirs) {
        break;
      }
    }
    
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATA_NODES).format(false).build();
      fail("Did not fail to start with all-corrupt logs");
    } catch (IllegalStateException ise) {
      GenericTestUtils.assertExceptionContains(
          "No non-corrupt logs for txid 3", ise);
    }
    cluster.shutdown();
  }

  
  private static class EditLogByteInputStream extends EditLogInputStream {
    private InputStream input;
    private long len;

    public EditLogByteInputStream(byte[] data) {
      len = data.length;
      input = new ByteArrayInputStream(data);
    }

    public int available() throws IOException {
      return input.available();
    }
    
    public int read() throws IOException {
      return input.read();
    }
    
    public long length() throws IOException {
      return len;
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
      return input.read(b, off, len);
    }

    public void close() throws IOException {
      input.close();
    }

    @Override // JournalStream
    public String getName() {
      return "AnonEditLogByteInputStream";
    }

    @Override // JournalStream
    public JournalType getType() {
      return JournalType.FILE;
    }
  }

  /*
   * TODO: need to re-enable this test in HDFS-1073 branch!
  public void testFailedOpen() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    final FSNamesystem fsn = cluster.getNamesystem();

    // Set up spys
    final FSImage originalImage = fsn.getFSImage();
    NNStorage storage = originalImage.getStorage();
    NNStorage spyStorage = spy(storage);
    originalImage.storage = spyStorage;
    
    final FSEditLog editLog = originalImage.getEditLog();
    FSEditLog spyLog = spy(editLog);

    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;
    spyImage.storage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));
        
    // Fail every attempt to open a new edit file
    doThrow(new IOException("Injected fault: open")).
      when(spyLog).addNewEditLogStream((File)anyObject());
    
    try {
      spyLog.close();
      spyLog.open();
      fail("open did not fail even when all directories failed!");
    } catch(IOException ioe) {
      LOG.info("Got expected exception", ioe);
    } finally {
      spyLog.close();
    }
    
    // Reset and try it with a working open
    Mockito.reset(spyLog);
    spyImage.storage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));
    spyLog.open();
    
    // Close everything off
    spyLog.close();
    originalImage.close();
    fsn.close();
  }
  */
}
