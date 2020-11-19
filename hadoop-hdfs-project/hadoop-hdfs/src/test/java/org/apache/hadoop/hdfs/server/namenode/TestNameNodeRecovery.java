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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

/**
 * This tests data recovery mode for the NameNode.
 */

@RunWith(Parameterized.class)
public class TestNameNodeRecovery {
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{ Boolean.FALSE });
    params.add(new Object[]{ Boolean.TRUE });
    return params;
  }

  private static boolean useAsyncEditLog;
  public TestNameNodeRecovery(Boolean async) {
    useAsyncEditLog = async;
  }

  private static Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        useAsyncEditLog);
    return conf;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodeRecovery.class);
  private static final StartupOption recoverStartOpt = StartupOption.RECOVER;
  private static final File TEST_DIR = PathUtils.getTestDir(TestNameNodeRecovery.class);

  static {
    recoverStartOpt.setForce(MetaRecoveryContext.FORCE_ALL);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  static void runEditLogTest(EditLogTestSetup elts) throws IOException {
    final File TEST_LOG_NAME = new File(TEST_DIR, "test_edit_log");
    final OpInstanceCache cache = new OpInstanceCache();
    
    EditLogFileOutputStream elfos = null;
    EditLogFileInputStream elfis = null;
    try {
      elfos = new EditLogFileOutputStream(getConf(), TEST_LOG_NAME, 0);
      elfos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

      elts.addTransactionsToLog(elfos, cache);
      elfos.setReadyToFlush();
      elfos.flushAndSync(true);
      elfos.close();
      elfos = null;
      elfis = new EditLogFileInputStream(TEST_LOG_NAME);
      elfis.setMaxOpSize(elts.getMaxOpSize());
      
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
      IOUtils.cleanupWithLogger(LOG, elfos, elfis);
    }
  }

  /**
   * A test scenario for the edit log
   */
  private static abstract class EditLogTestSetup {
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
    
    /**
     * Return the maximum opcode size we will use for input.
     */
    public int getMaxOpSize() {
      return DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;
    }
  }
  
  static void padEditLog(EditLogOutputStream elos, int paddingLength)
      throws IOException {
    if (paddingLength <= 0) {
      return;
    }
    byte buf[] = new byte[4096];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte)-1;
    }
    int pad = paddingLength;
    while (pad > 0) {
      int toWrite = pad > buf.length ? buf.length : pad;
      elos.writeRaw(buf, 0, toWrite);
      pad -= toWrite;
    }
  }

  static void addDeleteOpcode(EditLogOutputStream elos,
        OpInstanceCache cache, long txId, String path) throws IOException {
    DeleteOp op = DeleteOp.getInstance(cache);
    op.setTransactionId(txId);
    op.setPath(path);
    op.setTimestamp(0);
    elos.write(op);
  }
  
  /**
   * Test the scenario where we have an empty edit log.
   *
   * This class is also useful in testing whether we can correctly handle
   * various amounts of padding bytes at the end of the log.  We should be
   * able to handle any amount of padding (including no padding) without
   * throwing an exception.
   */
  private static class EltsTestEmptyLog extends EditLogTestSetup {
    private final int paddingLength;

    public EltsTestEmptyLog(int paddingLength) {
      this.paddingLength = paddingLength;
    }

    @Override
    public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException {
      padEditLog(elos, paddingLength);
    }

    @Override
    public long getLastValidTxId() {
      return -1;
    }

    @Override
    public Set<Long> getValidTxIds() {
      return new HashSet<Long>();
    } 
  }
  
  /** Test an empty edit log */
  @Test(timeout=180000)
  public void testEmptyLog() throws IOException {
    runEditLogTest(new EltsTestEmptyLog(0));
  }

  /** Test an empty edit log with padding */
  @Test(timeout=180000)
  public void testEmptyPaddedLog() throws IOException {
    runEditLogTest(new EltsTestEmptyLog(
        EditLogFileOutputStream.MIN_PREALLOCATION_LENGTH));
  }
  
  /** Test an empty edit log with extra-long padding */
  @Test(timeout=180000)
  public void testEmptyExtraPaddedLog() throws IOException {
    runEditLogTest(new EltsTestEmptyLog(
        3 * EditLogFileOutputStream.MIN_PREALLOCATION_LENGTH));
  }

  /**
   * Test using a non-default maximum opcode length.
   */
  private static class EltsTestNonDefaultMaxOpSize extends EditLogTestSetup {
    public EltsTestNonDefaultMaxOpSize() {
    }

    @Override
    public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException {
      addDeleteOpcode(elos, cache, 0, "/foo");
      addDeleteOpcode(elos, cache, 1,
       "/supercalifragalisticexpialadocius.supercalifragalisticexpialadocius");
    }

    @Override
    public long getLastValidTxId() {
      return 0;
    }

    @Override
    public Set<Long> getValidTxIds() {
      return Sets.newHashSet(0L);
    } 
    
    public int getMaxOpSize() {
      return 40;
    }
  }

  /** Test an empty edit log with extra-long padding */
  @Test(timeout=180000)
  public void testNonDefaultMaxOpSize() throws IOException {
    runEditLogTest(new EltsTestNonDefaultMaxOpSize());
  }

  /**
   * Test the scenario where an edit log contains some padding (0xff) bytes
   * followed by valid opcode data.
   *
   * These edit logs are corrupt, but all the opcodes should be recoverable
   * with recovery mode.
   */
  private static class EltsTestOpcodesAfterPadding extends EditLogTestSetup {
    private final int paddingLength;

    public EltsTestOpcodesAfterPadding(int paddingLength) {
      this.paddingLength = paddingLength;
    }

    @Override
    public void addTransactionsToLog(EditLogOutputStream elos,
        OpInstanceCache cache) throws IOException {
      padEditLog(elos, paddingLength);
      addDeleteOpcode(elos, cache, 0, "/foo");
    }

    @Override
    public long getLastValidTxId() {
      return 0;
    }

    @Override
    public Set<Long> getValidTxIds() {
      return Sets.newHashSet(0L);
    } 
  }

  @Test(timeout=180000)
  public void testOpcodesAfterPadding() throws IOException {
    runEditLogTest(new EltsTestOpcodesAfterPadding(
        EditLogFileOutputStream.MIN_PREALLOCATION_LENGTH));
  }

  @Test(timeout=180000)
  public void testOpcodesAfterExtraPadding() throws IOException {
    runEditLogTest(new EltsTestOpcodesAfterPadding(
        3 * EditLogFileOutputStream.MIN_PREALLOCATION_LENGTH));
  }

  private static class EltsTestGarbageInEditLog extends EditLogTestSetup {
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
    private final byte padByte;

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

  /**
   * Create a test configuration that will exercise the initializeGenericKeys
   * code path.  This is a regression test for HDFS-4279.
   */
  static void setupRecoveryTestConf(Configuration conf) throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
      "ns1"), "nn1,nn2");
    String baseDir = GenericTestUtils.getTestDir("setupRecoveryTestConf")
        .getAbsolutePath();
    File nameDir = new File(baseDir, "nameR");
    File secondaryDir = new File(baseDir, "namesecondaryR");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.
        DFS_NAMENODE_NAME_DIR_KEY, "ns1", "nn1"),
        nameDir.getCanonicalPath());
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.
        DFS_NAMENODE_CHECKPOINT_DIR_KEY, "ns1", "nn1"),
        secondaryDir.getCanonicalPath());
    conf.unset(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    FileUtils.deleteQuietly(nameDir);
    if (!nameDir.mkdirs()) {
      throw new RuntimeException("failed to make directory " +
        nameDir.getAbsolutePath());
    }
    FileUtils.deleteQuietly(secondaryDir);
    if (!secondaryDir.mkdirs()) {
      throw new RuntimeException("failed to make directory " +
        secondaryDir.getAbsolutePath());
    }
  }

  static void testNameNodeRecoveryImpl(Corruptor corruptor, boolean finalize)
      throws IOException {
    final String TEST_PATH = "/test/path/dir";
    final String TEST_PATH2 = "/second/dir";
    final boolean needRecovery = corruptor.needRecovery(finalize);

    // start a cluster
    Configuration conf = getConf();
    setupRecoveryTestConf(conf);
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    StorageDirectory sd = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .manageNameDfsDirs(false).build();
      cluster.waitActive();
      if (!finalize) {
        // Normally, the in-progress edit log would be finalized by
        // FSEditLog#endCurrentLogSegment.  For testing purposes, we
        // disable that here.
        FSEditLog spyLog =
            spy(cluster.getNameNode().getFSImage().getEditLog());
        doNothing().when(spyLog).endCurrentLogSegment(true);
        DFSTestUtil.setEditLogForTesting(cluster.getNamesystem(), spyLog);
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
          .enableManagedDfsDirsRedundancy(false).format(false).build();
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
          .enableManagedDfsDirsRedundancy(false).format(false)
          .startupOption(recoverStartOpt).build();
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
          .enableManagedDfsDirsRedundancy(false).format(false).build();
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
