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

import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_FAIL;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_ROLL;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.setupEdits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalManager.CorruptionException;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.TestEditLog.AbortSpec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class TestFileJournalManager {
  static final Log LOG = LogFactory.getLog(TestFileJournalManager.class);

  private Configuration conf;

  static {
    // No need to fsync for the purposes of tests. This makes
    // the tests run much faster.
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  /**
   * Find out how many transactions we can read from a
   * FileJournalManager, starting at a given transaction ID.
   * 
   * @param jm              The journal manager
   * @param fromTxId        Transaction ID to start at
   * @param inProgressOk    Should we consider edit logs that are not finalized?
   * @return                The number of transactions
   * @throws IOException
   */
  static long getNumberOfTransactions(FileJournalManager jm, long fromTxId,
      boolean inProgressOk, boolean abortOnGap) throws IOException {
    long numTransactions = 0, txId = fromTxId;
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    jm.selectInputStreams(allStreams, fromTxId, inProgressOk);
    EditLogInputStream elis = null;
    try {
      while ((elis = allStreams.poll()) != null) {
        try {
          elis.skipUntil(txId);
          while (true) {
            FSEditLogOp op = elis.readOp();
            if (op == null) {
              break;
            }
            if (abortOnGap && (op.getTransactionId() != txId)) {
              LOG.info("getNumberOfTransactions: detected gap at txId "
                  + fromTxId);
              return numTransactions;
            }
            txId = op.getTransactionId() + 1;
            numTransactions++;
          }
        } finally {
          IOUtils.cleanup(LOG, elis);
        }
      }
    } finally {
      IOUtils.cleanup(LOG, allStreams.toArray(new EditLogInputStream[0]));
    }
    return numTransactions;
  }
  
  /** 
   * Test the normal operation of loading transactions from
   * file journal manager. 3 edits directories are setup without any
   * failures. Test that we read in the expected number of transactions.
   */
  @Test
  public void testNormalOperation() throws IOException {
    File f1 = new File(TestEditLog.TEST_DIR + "/normtest0");
    File f2 = new File(TestEditLog.TEST_DIR + "/normtest1");
    File f3 = new File(TestEditLog.TEST_DIR + "/normtest2");
    
    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI(), f3.toURI());
    NNStorage storage = setupEdits(editUris, 5);
    
    long numJournals = 0;
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      FileJournalManager jm = new FileJournalManager(conf, sd, storage);
      assertEquals(6*TXNS_PER_ROLL, getNumberOfTransactions(jm, 1, true, false));
      numJournals++;
    }
    assertEquals(3, numJournals);
  }

  /**
   * Test that inprogress files are handled correct. Set up a single
   * edits directory. Fail on after the last roll. Then verify that the 
   * logs have the expected number of transactions.
   */
  @Test
  public void testInprogressRecovery() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/inprogressrecovery");
    // abort after the 5th roll 
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   5, new AbortSpec(5, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, 
                 getNumberOfTransactions(jm, 1, true, false));
  }

  /**
   * Test a mixture of inprogress files and finalised. Set up 3 edits 
   * directories and fail the second on the last roll. Verify that reading
   * the transactions, reads from the finalised directories.
   */
  @Test
  public void testInprogressRecoveryMixed() throws IOException {
    File f1 = new File(TestEditLog.TEST_DIR + "/mixtest0");
    File f2 = new File(TestEditLog.TEST_DIR + "/mixtest1");
    File f3 = new File(TestEditLog.TEST_DIR + "/mixtest2");
    
    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI(), f3.toURI());

    // abort after the 5th roll 
    NNStorage storage = setupEdits(editUris,
                                   5, new AbortSpec(5, 1));
    Iterator<StorageDirectory> dirs = storage.dirIterator(NameNodeDirType.EDITS);
    StorageDirectory sd = dirs.next();
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    assertEquals(6*TXNS_PER_ROLL, getNumberOfTransactions(jm, 1, true, false));
    
    sd = dirs.next();
    jm = new FileJournalManager(conf, sd, storage);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, getNumberOfTransactions(jm, 1,
        true, false));

    sd = dirs.next();
    jm = new FileJournalManager(conf, sd, storage);
    assertEquals(6*TXNS_PER_ROLL, getNumberOfTransactions(jm, 1, true, false));
  }

  /** 
   * Test that FileJournalManager behaves correctly despite inprogress
   * files in all its edit log directories. Set up 3 directories and fail
   * all on the last roll. Verify that the correct number of transaction 
   * are then loaded.
   */
  @Test
  public void testInprogressRecoveryAll() throws IOException {
    File f1 = new File(TestEditLog.TEST_DIR + "/failalltest0");
    File f2 = new File(TestEditLog.TEST_DIR + "/failalltest1");
    File f3 = new File(TestEditLog.TEST_DIR + "/failalltest2");
    
    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI(), f3.toURI());
    // abort after the 5th roll 
    NNStorage storage = setupEdits(editUris, 5, 
                                   new AbortSpec(5, 0),
                                   new AbortSpec(5, 1),
                                   new AbortSpec(5, 2));
    Iterator<StorageDirectory> dirs = storage.dirIterator(NameNodeDirType.EDITS);
    StorageDirectory sd = dirs.next();
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, getNumberOfTransactions(jm, 1,
        true, false));
    
    sd = dirs.next();
    jm = new FileJournalManager(conf, sd, storage);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, getNumberOfTransactions(jm, 1,
        true, false));

    sd = dirs.next();
    jm = new FileJournalManager(conf, sd, storage);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, getNumberOfTransactions(jm, 1,
        true, false));
  }

  /** 
   * Corrupt an edit log file after the start segment transaction
   */
  private void corruptAfterStartSegment(File f) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(0x20); // skip version and first tranaction and a bit of next transaction
    for (int i = 0; i < 1000; i++) {
      raf.writeInt(0xdeadbeef);
    }
    raf.close();
  }
  
  @Test(expected=IllegalStateException.class)
  public void testFinalizeErrorReportedToNNStorage() throws IOException, InterruptedException {
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltestError");
    // abort after 10th roll
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    String sdRootPath = sd.getRoot().getAbsolutePath();
    FileUtil.chmod(sdRootPath, "-w", true);
    try {
      jm.finalizeLogSegment(0, 1);
    } finally {
      FileUtil.chmod(sdRootPath, "+w", true);
      assertTrue(storage.getRemovedStorageDirs().contains(sd));
    }
  }

  /** 
   * Test that we can read from a stream created by FileJournalManager.
   * Create a single edits directory, failing it on the final roll.
   * Then try loading from the point of the 3rd roll. Verify that we read 
   * the correct number of transactions from this point.
   */
  @Test 
  public void testReadFromStream() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/readfromstream");
    // abort after 10th roll
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    long expectedTotalTxnCount = TXNS_PER_ROLL*10 + TXNS_PER_FAIL;
    assertEquals(expectedTotalTxnCount, getNumberOfTransactions(jm, 1,
        true, false));

    long skippedTxns = (3*TXNS_PER_ROLL); // skip first 3 files
    long startingTxId = skippedTxns + 1; 

    long numLoadable = getNumberOfTransactions(jm, startingTxId,
        true, false);
    assertEquals(expectedTotalTxnCount - skippedTxns, numLoadable); 
  }

  /**
   * Make requests with starting transaction ids which don't match the beginning
   * txid of some log segments.
   * 
   * This should succeed.
   */
  @Test
  public void testAskForTransactionsMidfile() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/askfortransactionsmidfile");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 
                                   10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    
    // 10 rolls, so 11 rolled files, 110 txids total.
    final int TOTAL_TXIDS = 10 * 11;
    for (int txid = 1; txid <= TOTAL_TXIDS; txid++) {
      assertEquals((TOTAL_TXIDS - txid) + 1, getNumberOfTransactions(jm, txid,
          true, false));
    }
  }

  /** 
   * Test that we receive the correct number of transactions when we count
   * the number of transactions around gaps.
   * Set up a single edits directory, with no failures. Delete the 4th logfile.
   * Test that getNumberOfTransactions returns the correct number of 
   * transactions before this gap and after this gap. Also verify that if you
   * try to count on the gap that an exception is thrown.
   */
  @Test
  public void testManyLogsWithGaps() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/manylogswithgaps");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    final long startGapTxId = 3*TXNS_PER_ROLL + 1;
    final long endGapTxId = 4*TXNS_PER_ROLL;
    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startGapTxId, endGapTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());
    
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    assertEquals(startGapTxId-1, getNumberOfTransactions(jm, 1, true, true));

    assertEquals(0, getNumberOfTransactions(jm, startGapTxId, true, true));

    // rolled 10 times so there should be 11 files.
    assertEquals(11*TXNS_PER_ROLL - endGapTxId, 
                 getNumberOfTransactions(jm, endGapTxId + 1, true, true));
  }

  /** 
   * Test that we can load an edits directory with a corrupt inprogress file.
   * The corrupt inprogress file should be moved to the side.
   */
  @Test
  public void testManyLogsWithCorruptInprogress() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/manylogswithcorruptinprogress");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_inprogress")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    
    corruptAfterStartSegment(files[0]);

    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    assertEquals(10*TXNS_PER_ROLL+1, 
                 getNumberOfTransactions(jm, 1, true, false));
  }

  @Test
  public void testGetRemoteEditLog() throws IOException {
    StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
        NameNodeDirType.EDITS, false,
        NNStorage.getFinalizedEditsFileName(1, 100),
        NNStorage.getFinalizedEditsFileName(101, 200),
        NNStorage.getInProgressEditsFileName(201),
        NNStorage.getFinalizedEditsFileName(1001, 1100));
        
    // passing null for NNStorage because this unit test will not use it
    FileJournalManager fjm = new FileJournalManager(conf, sd, null);
    assertEquals("[1,100],[101,200],[1001,1100]", getLogsAsString(fjm, 1));
    assertEquals("[101,200],[1001,1100]", getLogsAsString(fjm, 101));
    assertEquals("[101,200],[1001,1100]", getLogsAsString(fjm, 150));
    assertEquals("[1001,1100]", getLogsAsString(fjm, 201));
    assertEquals("Asking for a newer log than exists should return empty list",
        "", getLogsAsString(fjm, 9999));
  }

  /**
   * tests that passing an invalid dir to matchEditLogs throws IOException 
   */
  @Test(expected = IOException.class)
  public void testMatchEditLogInvalidDirThrowsIOException() throws IOException {
    File badDir = new File("does not exist");
    FileJournalManager.matchEditLogs(badDir);
  }
  
  private static EditLogInputStream getJournalInputStream(FileJournalManager jm,
      long txId, boolean inProgressOk) throws IOException {
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    jm.selectInputStreams(allStreams, txId, inProgressOk);
    EditLogInputStream elis = null, ret;
    try {
      while ((elis = allStreams.poll()) != null) {
        if (elis.getFirstTxId() > txId) {
          break;
        }
        if (elis.getLastTxId() < txId) {
          elis.close();
          continue;
        }
        elis.skipUntil(txId);
        ret = elis;
        elis = null;
        return ret;
      }
    } finally {
      IOUtils.cleanup(LOG,  allStreams.toArray(new EditLogInputStream[0]));
      IOUtils.cleanup(LOG,  elis);
    }
    return null;
  }
    
  /**
   * Make sure that we starting reading the correct op when we request a stream
   * with a txid in the middle of an edit log file.
   */
  @Test
  public void testReadFromMiddleOfEditLog() throws CorruptionException,
      IOException {
    File f = new File(TestEditLog.TEST_DIR + "/readfrommiddleofeditlog");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 
                                   10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    
    EditLogInputStream elis = getJournalInputStream(jm, 5, true);
    try {
      FSEditLogOp op = elis.readOp();
      assertEquals("read unexpected op", op.getTransactionId(), 5);
    } finally {
      IOUtils.cleanup(LOG, elis);
    }
  }

  /**
   * Make sure that in-progress streams aren't counted if we don't ask for
   * them.
   */
  @Test
  public void testExcludeInProgressStreams() throws CorruptionException,
      IOException {
    File f = new File(TestEditLog.TEST_DIR + "/excludeinprogressstreams");
    
    // Don't close the edit log once the files have been set up.
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 
                                   10, false);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(conf, sd, storage);
    
    // If we exclude the in-progess stream, we should only have 100 tx.
    assertEquals(100, getNumberOfTransactions(jm, 1, false, false));
    
    EditLogInputStream elis = getJournalInputStream(jm, 90, false);
    try {
      FSEditLogOp lastReadOp = null;
      while ((lastReadOp = elis.readOp()) != null) {
        assertTrue(lastReadOp.getTransactionId() <= 100);
      }
    } finally {
      IOUtils.cleanup(LOG, elis);
    }
  }

  /**
   * Tests that internal renames are done using native code on platforms that
   * have it.  The native rename includes more detailed information about the
   * failure, which can be useful for troubleshooting.
   */
  @Test
  public void testDoPreUpgradeIOError() throws IOException {
    File storageDir = new File(TestEditLog.TEST_DIR, "preupgradeioerror");
    List<URI> editUris = Collections.singletonList(storageDir.toURI());
    NNStorage storage = setupEdits(editUris, 5);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    assertNotNull(sd);
    // Change storage directory so that renaming current to previous.tmp fails.
    FileUtil.setWritable(storageDir, false);
    FileJournalManager jm = null;
    try {
      jm = new FileJournalManager(conf, sd, storage);
      exception.expect(IOException.class);
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        exception.expectMessage("failure in native rename");
      }
      jm.doPreUpgrade();
    } finally {
      IOUtils.cleanup(LOG, jm);
      // Restore permissions on storage directory and make sure we can delete.
      FileUtil.setWritable(storageDir, true);
      FileUtil.fullyDelete(storageDir);
    }
  }

  private static String getLogsAsString(
      FileJournalManager fjm, long firstTxId) throws IOException {
    return Joiner.on(",").join(fjm.getRemoteEditLogs(firstTxId, false));
  }
}
