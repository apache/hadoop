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
package org.apache.hadoop.hdfs.qjournal.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.*;
import org.mockito.Mockito;

public class TestJournal {
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);
  private static final NamespaceInfo FAKE_NSINFO_2 = new NamespaceInfo(
      6789, "mycluster", "my-bp", 0L);
  
  private static final String JID = "test-journal";

  private static final File TEST_LOG_DIR = new File(
      new File(MiniDFSCluster.getBaseDirectory()), "TestJournal");

  private StorageErrorReporter mockErrorReporter = Mockito.mock(
      StorageErrorReporter.class);

  private Journal journal;

  
  @Before
  public void setup() throws Exception {
    FileUtil.fullyDelete(TEST_LOG_DIR);
    journal = new Journal(TEST_LOG_DIR, JID, mockErrorReporter);
    journal.format(FAKE_NSINFO);
  }
  
  @After
  public void verifyNoStorageErrors() throws Exception{
    Mockito.verify(mockErrorReporter, Mockito.never())
      .reportErrorOnFile(Mockito.<File>any());
  }
  
  @After
  public void cleanup() {
    IOUtils.closeStream(journal);
  }
  
  @Test (timeout = 10000)
  public void testEpochHandling() throws Exception {
    assertEquals(0, journal.getLastPromisedEpoch());
    NewEpochResponseProto newEpoch =
        journal.newEpoch(FAKE_NSINFO, 1);
    assertFalse(newEpoch.hasLastSegmentTxId());
    assertEquals(1, journal.getLastPromisedEpoch());
    journal.newEpoch(FAKE_NSINFO, 3);
    assertFalse(newEpoch.hasLastSegmentTxId());
    assertEquals(3, journal.getLastPromisedEpoch());
    try {
      journal.newEpoch(FAKE_NSINFO, 3);
      fail("Should have failed to promise same epoch twice");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Proposed epoch 3 <= last promise 3", ioe);
    }
    try {
      journal.startLogSegment(makeRI(1), 12345L);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
    try {
      journal.journal(makeRI(1), 12345L, 100L, 0, new byte[0]);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
  }
  
  @Test (timeout = 10000)
  public void testMaintainCommittedTxId() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    journal.startLogSegment(makeRI(1), 1);
    // Send txids 1-3, with a request indicating only 0 committed
    journal.journal(new RequestInfo(JID, 1, 2, 0), 1, 1, 3,
        QJMTestUtil.createTxnData(1, 3));
    assertEquals(0, journal.getCommittedTxnIdForTests());
    
    // Send 4-6, with request indicating that through 3 is committed.
    journal.journal(new RequestInfo(JID, 1, 3, 3), 1, 4, 3,
        QJMTestUtil.createTxnData(4, 6));
    assertEquals(3, journal.getCommittedTxnIdForTests());    
  }
  
  @Test (timeout = 10000)
  public void testRestartJournal() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    journal.startLogSegment(makeRI(1), 1);
    journal.journal(makeRI(2), 1, 1, 2, 
        QJMTestUtil.createTxnData(1, 2));
    // Don't finalize.
    
    String storageString = journal.getStorage().toColonSeparatedString();
    System.err.println("storage string: " + storageString);
    journal.close(); // close to unlock the storage dir
    
    // Now re-instantiate, make sure history is still there
    journal = new Journal(TEST_LOG_DIR, JID, mockErrorReporter);
    
    // The storage info should be read, even if no writer has taken over.
    assertEquals(storageString,
        journal.getStorage().toColonSeparatedString());

    assertEquals(1, journal.getLastPromisedEpoch());
    NewEpochResponseProtoOrBuilder newEpoch = journal.newEpoch(FAKE_NSINFO, 2);
    assertEquals(1, newEpoch.getLastSegmentTxId());
  }
  
  @Test (timeout = 10000)
  public void testFormatResetsCachedValues() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 12345L);
    journal.startLogSegment(new RequestInfo(JID, 12345L, 1L, 0L), 1L);

    assertEquals(12345L, journal.getLastPromisedEpoch());
    assertEquals(12345L, journal.getLastWriterEpoch());
    assertTrue(journal.isFormatted());
    
    // Close the journal in preparation for reformatting it.
    journal.close();
    journal.format(FAKE_NSINFO_2);
    
    assertEquals(0, journal.getLastPromisedEpoch());
    assertEquals(0, journal.getLastWriterEpoch());
    assertTrue(journal.isFormatted());
  }
  
  /**
   * Test that, if the writer crashes at the very beginning of a segment,
   * before any transactions are written, that the next newEpoch() call
   * returns the prior segment txid as its most recent segment.
   */
  @Test (timeout = 10000)
  public void testNewEpochAtBeginningOfSegment() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    journal.startLogSegment(makeRI(1), 1);
    journal.journal(makeRI(2), 1, 1, 2, 
        QJMTestUtil.createTxnData(1, 2));
    journal.finalizeLogSegment(makeRI(3), 1, 2);
    journal.startLogSegment(makeRI(4), 3);
    NewEpochResponseProto resp = journal.newEpoch(FAKE_NSINFO, 2);
    assertEquals(1, resp.getLastSegmentTxId());
  }
  
  @Test (timeout = 10000)
  public void testJournalLocking() throws Exception {
    Assume.assumeTrue(journal.getStorage().getStorageDir(0).isLockSupported());
    StorageDirectory sd = journal.getStorage().getStorageDir(0);
    File lockFile = new File(sd.getRoot(), Storage.STORAGE_FILE_LOCK);
    
    // Journal should be locked, since the format() call locks it.
    GenericTestUtils.assertExists(lockFile);

    journal.newEpoch(FAKE_NSINFO,  1);
    try {
      new Journal(TEST_LOG_DIR, JID, mockErrorReporter);
      fail("Did not fail to create another journal in same dir");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot lock storage", ioe);
    }
    
    journal.close();
    
    // Journal should no longer be locked after the close() call.
    // Hence, should be able to create a new Journal in the same dir.
    Journal journal2 = new Journal(TEST_LOG_DIR, JID, mockErrorReporter);
    journal2.newEpoch(FAKE_NSINFO, 2);
    journal2.close();
  }
  
  /**
   * Test finalizing a segment after some batch of edits were missed.
   * This should fail, since we validate the log before finalization.
   */
  @Test (timeout = 10000)
  public void testFinalizeWhenEditsAreMissed() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    journal.startLogSegment(makeRI(1), 1);
    journal.journal(makeRI(2), 1, 1, 3,
        QJMTestUtil.createTxnData(1, 3));
    
    // Try to finalize up to txn 6, even though we only wrote up to txn 3.
    try {
      journal.finalizeLogSegment(makeRI(3), 1, 6);
      fail("did not fail to finalize");
    } catch (JournalOutOfSyncException e) {
      GenericTestUtils.assertExceptionContains(
          "but only written up to txid 3", e);
    }
    
    // Check that, even if we re-construct the journal by scanning the
    // disk, we don't allow finalizing incorrectly.
    journal.close();
    journal = new Journal(TEST_LOG_DIR, JID, mockErrorReporter);
    
    try {
      journal.finalizeLogSegment(makeRI(4), 1, 6);
      fail("did not fail to finalize");
    } catch (JournalOutOfSyncException e) {
      GenericTestUtils.assertExceptionContains(
          "disk only contains up to txid 3", e);
    }
  }
  
  /**
   * Ensure that finalizing a segment which doesn't exist throws the
   * appropriate exception.
   */
  @Test (timeout = 10000)
  public void testFinalizeMissingSegment() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    try {
      journal.finalizeLogSegment(makeRI(1), 1000, 1001);
      fail("did not fail to finalize");
    } catch (JournalOutOfSyncException e) {
      GenericTestUtils.assertExceptionContains(
          "No log file to finalize at transaction ID 1000", e);
    }
  }

  /**
   * Assume that a client is writing to a journal, but loses its connection
   * in the middle of a segment. Thus, any future journal() calls in that
   * segment may fail, because some txns were missed while the connection was
   * down.
   *
   * Eventually, the connection comes back, and the NN tries to start a new
   * segment at a higher txid. This should abort the old one and succeed.
   */
  @Test (timeout = 10000)
  public void testAbortOldSegmentIfFinalizeIsMissed() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    
    // Start a segment at txid 1, and write a batch of 3 txns.
    journal.startLogSegment(makeRI(1), 1);
    journal.journal(makeRI(2), 1, 1, 3,
        QJMTestUtil.createTxnData(1, 3));

    GenericTestUtils.assertExists(
        journal.getStorage().getInProgressEditLog(1));
    
    // Try to start new segment at txid 6, this should abort old segment and
    // then succeed, allowing us to write txid 6-9.
    journal.startLogSegment(makeRI(3), 6);
    journal.journal(makeRI(4), 6, 6, 3,
        QJMTestUtil.createTxnData(6, 3));

    // The old segment should *not* be finalized.
    GenericTestUtils.assertExists(
        journal.getStorage().getInProgressEditLog(1));
    GenericTestUtils.assertExists(
        journal.getStorage().getInProgressEditLog(6));
  }
  
  /**
   * Test behavior of startLogSegment() when a segment with the
   * same transaction ID already exists.
   */
  @Test (timeout = 10000)
  public void testStartLogSegmentWhenAlreadyExists() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    
    // Start a segment at txid 1, and write just 1 transaction. This
    // would normally be the START_LOG_SEGMENT transaction.
    journal.startLogSegment(makeRI(1), 1);
    journal.journal(makeRI(2), 1, 1, 1,
        QJMTestUtil.createTxnData(1, 1));
    
    // Try to start new segment at txid 1, this should succeed, because
    // we are allowed to re-start a segment if we only ever had the
    // START_LOG_SEGMENT transaction logged.
    journal.startLogSegment(makeRI(3), 1);
    journal.journal(makeRI(4), 1, 1, 1,
        QJMTestUtil.createTxnData(1, 1));

    // This time through, write more transactions afterwards, simulating
    // real user transactions.
    journal.journal(makeRI(5), 1, 2, 3,
        QJMTestUtil.createTxnData(2, 3));

    try {
      journal.startLogSegment(makeRI(6), 1);
      fail("Did not fail to start log segment which would overwrite " +
          "an existing one");
    } catch (IllegalStateException ise) {
      GenericTestUtils.assertExceptionContains(
          "seems to contain valid transactions", ise);
    }
    
    journal.finalizeLogSegment(makeRI(7), 1, 4);
    
    // Ensure that we cannot overwrite a finalized segment
    try {
      journal.startLogSegment(makeRI(8), 1);
      fail("Did not fail to start log segment which would overwrite " +
          "an existing one");
    } catch (IllegalStateException ise) {
      GenericTestUtils.assertExceptionContains(
          "have a finalized segment", ise);
    }

  }
  
  private static RequestInfo makeRI(int serial) {
    return new RequestInfo(JID, 1, serial, 0);
  }
  
  @Test (timeout = 10000)
  public void testNamespaceVerification() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);

    try {
      journal.newEpoch(FAKE_NSINFO_2, 2);
      fail("Did not fail newEpoch() when namespaces mismatched");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Incompatible namespaceID", ioe);
    }
  }

}
