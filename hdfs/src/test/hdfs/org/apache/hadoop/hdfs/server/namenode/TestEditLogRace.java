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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;

import static org.junit.Assert.*;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

/**
 * This class tests various synchronization bugs in FSEditLog rolling
 * and namespace saving.
 */
public class TestEditLogRace {
  private static final Log LOG = LogFactory.getLog(TestEditLogRace.class);

  private static final String NAME_DIR =
    MiniDFSCluster.getBaseDirectory() + "name1";

  // This test creates NUM_THREADS threads and each thread continuously writes
  // transactions
  static final int NUM_THREADS = 16;

  /**
   * The number of times to roll the edit log during the test. Since this
   * tests for a race condition, higher numbers are more likely to find
   * a bug if it exists, but the test will take longer.
   */
  static final int NUM_ROLLS = 30;

  /**
   * The number of times to save the fsimage and create an empty edit log.
   */
  static final int NUM_SAVE_IMAGE = 30;

  private List<Transactions> workers = new ArrayList<Transactions>();

  private static final int NUM_DATA_NODES = 1;

  /**
   * Several of the test cases work by introducing a sleep
   * into an operation that is usually fast, and then verifying
   * that another operation blocks for at least this amount of time.
   * This value needs to be significantly longer than the average
   * time for an fsync() or enterSafeMode().
   */
  private static final int BLOCK_TIME = 10;
  
  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    FSNamesystem namesystem;
    short replication = 3;
    long blockSize = 64;
    volatile boolean stopped = false;
    volatile Thread thr;
    AtomicReference<Throwable> caught;

    Transactions(FSNamesystem ns, AtomicReference<Throwable> caught) {
      namesystem = ns;
      this.caught = caught;
    }

    // add a bunch of transactions.
    public void run() {
      thr = Thread.currentThread();
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      int i = 0;
      while (!stopped) {
        try {
          String dirname = "/thr-" + thr.getId() + "-dir-" + i; 
          namesystem.mkdirs(dirname, p, true);
          namesystem.delete(dirname, true);
        } catch (SafeModeException sme) {
          // This is OK - the tests will bring NN in and out of safemode
        } catch (Throwable e) {
          LOG.warn("Got error in transaction thread", e);
          caught.compareAndSet(null, e);
          break;
        }
        i++;
      }
    }

    public void stop() {
      stopped = true;
    }

    public Thread getThread() {
      return thr;
    }
  }

  private void startTransactionWorkers(FSNamesystem namesystem,
                                       AtomicReference<Throwable> caughtErr) {
    // Create threads and make them run transactions concurrently.
    for (int i = 0; i < NUM_THREADS; i++) {
      Transactions trans = new Transactions(namesystem, caughtErr);
      new Thread(trans, "TransactionThread-" + i).start();
      workers.add(trans);
    }
  }

  private void stopTransactionWorkers() {
    // wait for all transactions to get over
    for (Transactions worker : workers) {
      worker.stop();
    }

    for (Transactions worker : workers) {
      Thread thr = worker.getThread();
      try {
        if (thr != null) thr.join();
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Tests rolling edit logs while transactions are ongoing.
   */
  @Test
  public void testEditLogRolling() throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;


    AtomicReference<Throwable> caughtErr = new AtomicReference<Throwable>();
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();

      // set small size of flush buffer
      editLog.setBufferCapacity(2048);
      editLog.close();
      editLog.open();

      startTransactionWorkers(namesystem, caughtErr);

      for (int i = 0; i < NUM_ROLLS && caughtErr.get() == null; i++) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {}

        LOG.info("Starting roll " + i + ".");
        editLog.rollEditLog();
        LOG.info("Roll complete " + i + ".");

        verifyEditLogs(namesystem, fsimage);

        LOG.info("Starting purge " + i + ".");
        editLog.purgeEditLog();
        LOG.info("Complete purge " + i + ".");
      }
    } finally {
      stopTransactionWorkers();
      if (caughtErr.get() != null) {
        throw new RuntimeException(caughtErr.get());
      }

      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }

  private void verifyEditLogs(FSNamesystem namesystem, FSImage fsimage)
    throws IOException {
    // Verify that we can read in all the transactions that we have written.
    // If there were any corruptions, it is likely that the reading in
    // of these transactions will throw an exception.
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      File editFile = FSImage.getImageFile(it.next(), NameNodeFile.EDITS);
      System.out.println("Verifying file: " + editFile);
      int numEdits = namesystem.getEditLog().loadFSEdits(
        new EditLogFileInputStream(editFile));
      System.out.println("Number of edits: " + numEdits);
    }
  }

  /**
   * Tests saving fs image while transactions are ongoing.
   */
  @Test
  public void testSaveNamespace() throws Exception {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;

    AtomicReference<Throwable> caughtErr = new AtomicReference<Throwable>();
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();

      // set small size of flush buffer
      editLog.setBufferCapacity(2048);
      editLog.close();
      editLog.open();

      startTransactionWorkers(namesystem, caughtErr);

      for (int i = 0; i < NUM_SAVE_IMAGE && caughtErr.get() == null; i++) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {}


        LOG.info("Save " + i + ": entering safe mode");
        namesystem.enterSafeMode();

        // Verify edit logs before the save
        verifyEditLogs(namesystem, fsimage);

        LOG.info("Save " + i + ": saving namespace");
        namesystem.saveNamespace();
        LOG.info("Save " + i + ": leaving safemode");

        // Verify that edit logs post save are also not corrupt
        verifyEditLogs(namesystem, fsimage);

        namesystem.leaveSafeMode(false);
        LOG.info("Save " + i + ": complete");

      }
    } finally {
      stopTransactionWorkers();
      if (caughtErr.get() != null) {
        throw new RuntimeException(caughtErr.get());
      }
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
 
  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, NAME_DIR);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, NAME_DIR);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false); 
    return conf;
  }


  /**
   * The logSync() method in FSEditLog is unsynchronized whiel syncing
   * so that other threads can concurrently enqueue edits while the prior
   * sync is ongoing. This test checks that the log is saved correctly
   * if the saveImage occurs while the syncing thread is in the unsynchronized middle section.
   * 
   * This replicates the following manual test proposed by Konstantin:
   *   I start the name-node in debugger.
   *   I do -mkdir and stop the debugger in logSync() just before it does flush.
   *   Then I enter safe mode with another client
   *   I start saveNamepsace and stop the debugger in
   *     FSImage.saveFSImage() -> FSEditLog.createEditLogFile()
   *     -> EditLogFileOutputStream.create() ->
   *     after truncating the file but before writing LAYOUT_VERSION into it.
   *   Then I let logSync() run.
   *   Then I terminate the name-node.
   *   After that the name-node wont start, since the edits file is broken.
   */
  @Test
  public void testSaveImageWhileSyncInProgress() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    NameNode.format(conf);
    final FSNamesystem namesystem = new FSNamesystem(conf);

    try {
      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();

      ArrayList<EditLogOutputStream> streams = editLog.getEditStreams();
      EditLogOutputStream spyElos = spy(streams.get(0));
      streams.set(0, spyElos);

      final AtomicReference<Throwable> deferredException =
          new AtomicReference<Throwable>();
      final CountDownLatch waitToEnterFlush = new CountDownLatch(1);
      
      final Thread doAnEditThread = new Thread() {
        public void run() {
          try {
            LOG.info("Starting mkdirs");
            namesystem.mkdirs("/test",
                new PermissionStatus("test","test", new FsPermission((short)00755)),
                true);
            LOG.info("mkdirs complete");
          } catch (Throwable ioe) {
            deferredException.set(ioe);
            waitToEnterFlush.countDown();
          }
        }
      };
      
      Answer<Void> blockingFlush = new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          LOG.info("Flush called");
          if (Thread.currentThread() == doAnEditThread) {
            LOG.info("edit thread: Telling main thread we made it to flush section...");
            // Signal to main thread that the edit thread is in the racy section
            waitToEnterFlush.countDown();
            LOG.info("edit thread: sleeping for " + BLOCK_TIME + "secs");
            Thread.sleep(BLOCK_TIME*1000);
            LOG.info("Going through to flush. This will allow the main thread to continue.");
          }
          invocation.callRealMethod();
          LOG.info("Flush complete");
          return null;
        }
      };
      doAnswer(blockingFlush).when(spyElos).flush();
      
      doAnEditThread.start();
      // Wait for the edit thread to get to the logsync unsynchronized section
      LOG.info("Main thread: waiting to enter flush...");
      waitToEnterFlush.await();
      assertNull(deferredException.get());
      LOG.info("Main thread: detected that logSync is in unsynchronized section.");
      LOG.info("Trying to enter safe mode.");
      LOG.info("This should block for " + BLOCK_TIME + "sec, since flush will sleep that long");
      
      long st = System.currentTimeMillis();
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      long et = System.currentTimeMillis();
      LOG.info("Entered safe mode");
      // Make sure we really waited for the flush to complete!
      assertTrue(et - st > (BLOCK_TIME - 1)*1000);

      // Once we're in safe mode, save namespace.
      namesystem.saveNamespace();

      LOG.info("Joining on edit thread...");
      doAnEditThread.join();
      assertNull(deferredException.get());

      verifyEditLogs(namesystem, fsimage);
    } finally {
      LOG.info("Closing namesystem");
      if(namesystem != null) namesystem.close();
    }
  }
  
  /**
   * Most of the FSNamesystem methods have a synchronized section where they
   * update the name system itself and write to the edit log, and then
   * unsynchronized, they call logSync. This test verifies that, if an
   * operation has written to the edit log but not yet synced it,
   * we wait for that sync before entering safe mode.
   */
  @Test
  public void testSaveRightBeforeSync() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    NameNode.format(conf);
    final FSNamesystem namesystem = new FSNamesystem(conf);

    try {
      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = spy(fsimage.getEditLog());
      fsimage.editLog = editLog;
      
      final AtomicReference<Throwable> deferredException =
          new AtomicReference<Throwable>();
      final CountDownLatch waitToEnterSync = new CountDownLatch(1);
      
      final Thread doAnEditThread = new Thread() {
        public void run() {
          try {
            LOG.info("Starting mkdirs");
            namesystem.mkdirs("/test",
                new PermissionStatus("test","test", new FsPermission((short)00755)),
                true);
            LOG.info("mkdirs complete");
          } catch (Throwable ioe) {
            deferredException.set(ioe);
            waitToEnterSync.countDown();
          }
        }
      };
      
      Answer<Void> blockingSync = new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          LOG.info("logSync called");
          if (Thread.currentThread() == doAnEditThread) {
            LOG.info("edit thread: Telling main thread we made it just before logSync...");
            waitToEnterSync.countDown();
            LOG.info("edit thread: sleeping for " + BLOCK_TIME + "secs");
            Thread.sleep(BLOCK_TIME*1000);
            LOG.info("Going through to logSync. This will allow the main thread to continue.");
          }
          invocation.callRealMethod();
          LOG.info("logSync complete");
          return null;
        }
      };
      doAnswer(blockingSync).when(editLog).logSync();
      
      doAnEditThread.start();
      LOG.info("Main thread: waiting to just before logSync...");
      waitToEnterSync.await();
      assertNull(deferredException.get());
      LOG.info("Main thread: detected that logSync about to be called.");
      LOG.info("Trying to enter safe mode.");
      LOG.info("This should block for " + BLOCK_TIME + "sec, since we have pending edits");
      
      long st = System.currentTimeMillis();
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      long et = System.currentTimeMillis();
      LOG.info("Entered safe mode");
      // Make sure we really waited for the flush to complete!
      assertTrue(et - st > (BLOCK_TIME - 1)*1000);

      // Once we're in safe mode, save namespace.
      namesystem.saveNamespace();

      LOG.info("Joining on edit thread...");
      doAnEditThread.join();
      assertNull(deferredException.get());

      verifyEditLogs(namesystem, fsimage);
    } finally {
      LOG.info("Closing namesystem");
      if(namesystem != null) namesystem.close();
    }
  }  
}
