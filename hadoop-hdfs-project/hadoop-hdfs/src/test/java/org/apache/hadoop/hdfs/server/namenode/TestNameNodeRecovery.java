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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * This tests data recovery mode for the NameNode.
 */
public class TestNameNodeRecovery {
  private static final Log LOG = LogFactory.getLog(TestNameNodeRecovery.class);
  private static StartupOption recoverStartOpt = StartupOption.RECOVER;

  static {
    recoverStartOpt.setForce(MetaRecoveryContext.FORCE_ALL);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  static void runEditLogTest(EditLogTestSetup elts) throws IOException {
    final String TEST_LOG_NAME = "test_edit_log";
    final OpInstanceCache cache = new OpInstanceCache();
    
    EditLogFileOutputStream elfos = null;
    File file = null;
    EditLogFileInputStream elfis = null;
    try {
      file = new File(TEST_LOG_NAME);
      elfos = new EditLogFileOutputStream(file, 0);
      elfos.create();

      elts.addTransactionsToLog(elfos, cache);
      elfos.setReadyToFlush();
      elfos.flushAndSync();
      elfos.close();
      elfos = null;
      file = new File(TEST_LOG_NAME);
      elfis = new EditLogFileInputStream(file);
      
      // reading through normally will get you an exception
      Set<Long> validTxIds = elts.getValidTxIds();
      FSEditLogOp op = null;
      long prevTxId = 0;
      try {
        while (true) {
          op = elfis.nextOp();
          if (op == null) {
            break;
          }
          LOG.debug("read txid " + op.txid);
          if (!validTxIds.contains(op.getTransactionId())) {
            fail("read txid " + op.getTransactionId() +
                ", which we did not expect to find.");
          }
          validTxIds.remove(op.getTransactionId());
          prevTxId = op.getTransactionId();
        }
        if (elts.getLastValidTxId() != -1) {
          fail("failed to throw IoException as expected");
        }
      } catch (IOException e) {
        if (elts.getLastValidTxId() == -1) {
          fail("expected all transactions to be valid, but got exception " +
              "on txid " + prevTxId);
        } else {
          assertEquals(prevTxId, elts.getLastValidTxId());
        }
      }
      
      if (elts.getLastValidTxId() != -1) {
        // let's skip over the bad transaction
        op = null;
        prevTxId = 0;
        try {
          while (true) {
            op = elfis.nextValidOp();
            if (op == null) {
              break;
            }
            prevTxId = op.getTransactionId();
            assertTrue(validTxIds.remove(op.getTransactionId()));
          }
        } catch (Throwable e) {
          fail("caught IOException while trying to skip over bad " +
              "transaction.   message was " + e.getMessage() + 
              "\nstack trace\n" + StringUtils.stringifyException(e));
        }
      }
      // We should have read every valid transaction.
      assertTrue(validTxIds.isEmpty());
    } finally {
      IOUtils.cleanup(LOG, elfos, elfis);
    }
  }

  private interface EditLogTestSetup {
    /** 
     * Set up the edit log.
     */
    abstract public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException;

    /**
     * Get the transaction ID right before the transaction which causes the
     * normal edit log loading process to bail out-- or -1 if the first
     * transaction should be bad.
     */
    abstract public long getLastValidTxId();

    /**
     * Get the transaction IDs which should exist and be valid in this
     * edit log.
     **/
    abstract public Set<Long> getValidTxIds();
  }
  
  private class EltsTestEmptyLog implements EditLogTestSetup {
    public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException {
        // do nothing
    }

    public long getLastValidTxId() {
      return -1;
    }

    public Set<Long> getValidTxIds() {
      return new HashSet<Long>();
    } 
  }
  
  /** Test an empty edit log */
  @Test(timeout=180000)
  public void testEmptyLog() throws IOException {
    runEditLogTest(new EltsTestEmptyLog());
  }
  
  private class EltsTestGarbageInEditLog implements EditLogTestSetup {
    final private long BAD_TXID = 4;
    final private long MAX_TXID = 10;
    
    @Override
    public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException {
      for (long txid = 1; txid <= MAX_TXID; txid++) {
        if (txid == BAD_TXID) {
          byte garbage[] = { 0x1, 0x2, 0x3 };
          elos.writeRaw(garbage, 0, garbage.length);
        }
        else {
          DeleteOp op;
          op = DeleteOp.getInstance(cache);
          op.setTransactionId(txid);
          op.setPath("/foo." + txid);
          op.setTimestamp(txid);
          elos.write(op);
        }
      }
    }

    @Override
    public long getLastValidTxId() {
      return BAD_TXID - 1;
    }

    @Override
    public Set<Long> getValidTxIds() {
      return Sets.newHashSet(1L , 2L, 3L, 5L, 6L, 7L, 8L, 9L, 10L);
    }
  }
  
  /** Test that we can successfully recover from a situation where there is
   * garbage in the middle of the edit log file output stream. */
  @Test(timeout=180000)
  public void testSkipEdit() throws IOException {
    runEditLogTest(new EltsTestGarbageInEditLog());
  }

  /**
   * An algorithm for corrupting an edit log.
   */
  static interface Corruptor {
    /*
     * Corrupt an edit log file.
     *
     * @param editFile   The edit log file
     */
    public void corrupt(File editFile) throws IOException;

    /*
     * Explain whether we need to read the log in recovery mode
     *
     * @param finalized  True if the edit log in question is finalized.
     *                   We're a little more lax about reading unfinalized
     *                   logs.  We will allow a small amount of garbage at
     *                   the end.  In a finalized log, every byte must be
     *                   perfect.
     *
     * @return           Whether we need to read the log in recovery mode
     */
    public boolean needRecovery(boolean finalized);

    /*
     * Get the name of this corruptor
     *
     * @return           The Corruptor name
     */
    public String getName();
  }

  static class TruncatingCorruptor implements Corruptor {
    @Override
    public void corrupt(File editFile) throws IOException {
      // Corrupt the last edit
      long fileLen = editFile.length();
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.setLength(fileLen - 1);
      rwf.close();
    }

    @Override
    public boolean needRecovery(boolean finalized) {
      return finalized;
    }

    @Override
    public String getName() {
      return "truncated";
    }
  }

  static class PaddingCorruptor implements Corruptor {
    @Override
    public void corrupt(File editFile) throws IOException {
      // Add junk to the end of the file
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(editFile.length());
      for (int i = 0; i < 129; i++) {
        rwf.write((byte)0);
      }
      rwf.write(0xd);
      rwf.write(0xe);
      rwf.write(0xa);
      rwf.write(0xd);
      rwf.close();
    }

    @Override
    public boolean needRecovery(boolean finalized) {
      // With finalized edit logs, we ignore what's at the end as long as we
      // can make it to the correct transaction ID.
      // With unfinalized edit logs, the finalization process ignores garbage
      // at the end.
      return false;
    }

    @Override
    public String getName() {
      return "padFatal";
    }
  }

  static class SafePaddingCorruptor implements Corruptor {
    private byte padByte;

    public SafePaddingCorruptor(byte padByte) {
      this.padByte = padByte;
      assert ((this.padByte == 0) || (this.padByte == -1));
    }

    @Override
    public void corrupt(File editFile) throws IOException {
      // Add junk to the end of the file
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(editFile.length());
      rwf.write((byte)-1);
      for (int i = 0; i < 1024; i++) {
        rwf.write(padByte);
      }
      rwf.close();
    }

    @Override
    public boolean needRecovery(boolean finalized) {
      return false;
    }

    @Override
    public String getName() {
      return "pad" + ((int)padByte);
    }
  }

  static void testNameNodeRecoveryImpl(Corruptor corruptor, boolean finalize)
      throws IOException {
    final String TEST_PATH = "/test/path/dir";
    final String TEST_PATH2 = "/second/dir";
    final boolean needRecovery = corruptor.needRecovery(finalize);

    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    StorageDirectory sd = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .build();
      cluster.waitActive();
      if (!finalize) {
        // Normally, the in-progress edit log would be finalized by
        // FSEditLog#endCurrentLogSegment.  For testing purposes, we
        // disable that here.
        FSEditLog spyLog =
            spy(cluster.getNameNode().getFSImage().getEditLog());
        doNothing().when(spyLog).endCurrentLogSegment(true);
        cluster.getNameNode().getFSImage().setEditLogForTesting(spyLog);
      }
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();
      FSImage fsimage = namesystem.getFSImage();
      fileSys.mkdirs(new Path(TEST_PATH));
      fileSys.mkdirs(new Path(TEST_PATH2));
      sd = fsimage.getStorage().dirIterator(NameNodeDirType.EDITS).next();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    File editFile = FSImageTestUtil.findLatestEditsLog(sd).getFile();
    assertTrue("Should exist: " + editFile, editFile.exists());

    // Corrupt the edit log
    LOG.info("corrupting edit log file '" + editFile + "'");
    corruptor.corrupt(editFile);

    // If needRecovery == true, make sure that we can't start the
    // cluster normally before recovery
    cluster = null;
    try {
      LOG.debug("trying to start normally (this should fail)...");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(false).build();
      cluster.waitActive();
      cluster.shutdown();
      if (needRecovery) {
        fail("expected the corrupted edit log to prevent normal startup");
      }
    } catch (IOException e) {
      if (!needRecovery) {
        LOG.error("Got unexpected failure with " + corruptor.getName() +
            corruptor, e);
        fail("got unexpected exception " + e.getMessage());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    // Perform NameNode recovery.
    // Even if there was nothing wrong previously (needRecovery == false),
    // this should still work fine.
    cluster = null;
    try {
      LOG.debug("running recovery...");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(false).startupOption(recoverStartOpt).build();
    } catch (IOException e) {
      fail("caught IOException while trying to recover. " +
          "message was " + e.getMessage() +
          "\nstack trace\n" + StringUtils.stringifyException(e));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    // Make sure that we can start the cluster normally after recovery
    cluster = null;
    try {
      LOG.debug("starting cluster normally after recovery...");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(false).build();
      LOG.debug("successfully recovered the " + corruptor.getName() +
          " corrupted edit log");
      cluster.waitActive();
      assertTrue(cluster.getFileSystem().exists(new Path(TEST_PATH)));
    } catch (IOException e) {
      fail("failed to recover.  Error message: " + e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /** Test that we can successfully recover from a situation where the last
   * entry in the edit log has been truncated. */
  @Test(timeout=180000)
  public void testRecoverTruncatedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new TruncatingCorruptor(), true);
    testNameNodeRecoveryImpl(new TruncatingCorruptor(), false);
  }

  /** Test that we can successfully recover from a situation where the last
   * entry in the edit log has been padded with garbage. */
  @Test(timeout=180000)
  public void testRecoverPaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new PaddingCorruptor(), true);
    testNameNodeRecoveryImpl(new PaddingCorruptor(), false);
  }

  /** Test that don't need to recover from a situation where the last
   * entry in the edit log has been padded with 0. */
  @Test(timeout=180000)
  public void testRecoverZeroPaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)0), true);
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)0), false);
  }

  /** Test that don't need to recover from a situation where the last
   * entry in the edit log has been padded with 0xff bytes. */
  @Test(timeout=180000)
  public void testRecoverNegativeOnePaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)-1), true);
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)-1), false);
  }
}
