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

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FilenameFilter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.test.GenericTestUtils;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.setupEdits;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.AbortSpec;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_ROLL;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_FAIL;

import com.google.common.collect.ImmutableList;
import com.google.common.base.Joiner;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

public class TestFileJournalManager {

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
      FileJournalManager jm = new FileJournalManager(sd);
      assertEquals(6*TXNS_PER_ROLL, jm.getNumberOfTransactions(1));
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
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltest0");
    // abort after the 5th roll 
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   5, new AbortSpec(5, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, 
                 jm.getNumberOfTransactions(1));
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
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(6*TXNS_PER_ROLL, jm.getNumberOfTransactions(1));
    
    sd = dirs.next();
    jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, jm.getNumberOfTransactions(1));

    sd = dirs.next();
    jm = new FileJournalManager(sd);
    assertEquals(6*TXNS_PER_ROLL, jm.getNumberOfTransactions(1));
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
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, jm.getNumberOfTransactions(1));
    
    sd = dirs.next();
    jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, jm.getNumberOfTransactions(1));

    sd = dirs.next();
    jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, jm.getNumberOfTransactions(1));
  }

  /** 
   * Corrupt an edit log file after the start segment transaction
   */
  private void corruptAfterStartSegment(File f) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(0x16); // skip version and first tranaction and a bit of next transaction
    for (int i = 0; i < 1000; i++) {
      raf.writeInt(0xdeadbeef);
    }
    raf.close();
  }

  /** 
   * Test that we can read from a stream created by FileJournalManager.
   * Create a single edits directory, failing it on the final roll.
   * Then try loading from the point of the 3rd roll. Verify that we read 
   * the correct number of transactions from this point.
   */
  @Test 
  public void testReadFromStream() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltest1");
    // abort after 10th roll
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(sd);
    long expectedTotalTxnCount = TXNS_PER_ROLL*10 + TXNS_PER_FAIL;
    assertEquals(expectedTotalTxnCount, jm.getNumberOfTransactions(1));

    long skippedTxns = (3*TXNS_PER_ROLL); // skip first 3 files
    long startingTxId = skippedTxns + 1; 

    long numTransactionsToLoad = jm.getNumberOfTransactions(startingTxId);
    long numLoaded = 0;
    while (numLoaded < numTransactionsToLoad) {
      EditLogInputStream editIn = jm.getInputStream(startingTxId);
      FSEditLogLoader.EditLogValidation val = FSEditLogLoader.validateEditLog(editIn);
      long count = val.getNumTransactions();

      editIn.close();
      startingTxId += count;
      numLoaded += count;
    }

    assertEquals(expectedTotalTxnCount - skippedTxns, numLoaded); 
  }

  /**
   * Try to make a request with a start transaction id which doesn't
   * match the start ID of some log segment. 
   * This should fail as edit logs must currently be treated as indevisable 
   * units.
   */
  @Test(expected=IOException.class)
  public void testAskForTransactionsMidfile() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltest2");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 
                                   10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(sd);
    jm.getNumberOfTransactions(2);    
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
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltest3");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    final long startGapTxId = 3*TXNS_PER_ROLL + 1;
    final long endGapTxId = 4*TXNS_PER_ROLL;
    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startGapTxId, endGapTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());
    
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(startGapTxId-1, jm.getNumberOfTransactions(1));

    try {
      jm.getNumberOfTransactions(startGapTxId);
      fail("Should have thrown an exception by now");
    } catch (IOException ioe) {
      assertTrue(true);
    }

    // rolled 10 times so there should be 11 files.
    assertEquals(11*TXNS_PER_ROLL - endGapTxId, 
                 jm.getNumberOfTransactions(endGapTxId+1));
  }

  /** 
   * Test that we can load an edits directory with a corrupt inprogress file.
   * The corrupt inprogress file should be moved to the side.
   */
  @Test
  public void testManyLogsWithCorruptInprogress() throws IOException {
    File f = new File(TestEditLog.TEST_DIR + "/filejournaltest5");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_inprogress")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    
    corruptAfterStartSegment(files[0]);

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(10*TXNS_PER_ROLL+1, 
                 jm.getNumberOfTransactions(1)); 
  }

  @Test
  public void testGetRemoteEditLog() throws IOException {
    StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
        NameNodeDirType.EDITS, false,
        NNStorage.getFinalizedEditsFileName(1, 100),
        NNStorage.getFinalizedEditsFileName(101, 200),
        NNStorage.getInProgressEditsFileName(201),
        NNStorage.getFinalizedEditsFileName(1001, 1100));
        
    FileJournalManager fjm = new FileJournalManager(sd);
    assertEquals("[1,100],[101,200],[1001,1100]", getLogsAsString(fjm, 1));
    assertEquals("[101,200],[1001,1100]", getLogsAsString(fjm, 101));
    assertEquals("[1001,1100]", getLogsAsString(fjm, 201));
    try {
      assertEquals("[]", getLogsAsString(fjm, 150));
      fail("Did not throw when asking for a txn in the middle of a log");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "150 which is in the middle", ioe);
    }
    assertEquals("Asking for a newer log than exists should return empty list",
        "", getLogsAsString(fjm, 9999));
  }

  private static String getLogsAsString(
      FileJournalManager fjm, long firstTxId) throws IOException {
    return Joiner.on(",").join(fjm.getRemoteEditLogs(firstTxId));
  }
}
