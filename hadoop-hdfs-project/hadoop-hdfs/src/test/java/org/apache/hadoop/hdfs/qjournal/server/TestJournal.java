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
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJournal {
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L, 0);
  private static final NamespaceInfo FAKE_NSINFO_2 = new NamespaceInfo(
      6789, "mycluster", "my-bp", 0L, 0);
  
  private static final String JID = "test-journal";

  private static final File TEST_LOG_DIR = new File(
      new File(MiniDFSCluster.getBaseDirectory()), "TestJournal");

  private StorageErrorReporter mockErrorReporter = Mockito.mock(
      StorageErrorReporter.class);

  private Journal journal;

  
  @Before
  public void setup() throws Exception {
    FileUtil.fullyDelete(TEST_LOG_DIR);
    journal = new Journal(TEST_LOG_DIR, mockErrorReporter);
  }
  
  @After
  public void verifyNoStorageErrors() throws Exception{
    Mockito.verify(mockErrorReporter, Mockito.never())
      .reportErrorOnFile(Mockito.<File>any());
  }
  
  @Test
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
      journal.startLogSegment(new RequestInfo(JID, 1L, 1L),
          12345L);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
    try {
      journal.journal(new RequestInfo(JID, 1L, 1L),
          100L, 0, new byte[0]);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
  }

  @Test
  public void testRestartJournal() throws Exception {
    journal.newEpoch(FAKE_NSINFO, 1);
    journal.startLogSegment(new RequestInfo("j", 1, 1), 1);
    journal.journal(new RequestInfo("j", 1, 2), 1, 2, 
        QJMTestUtil.createTxnData(1, 2));
    // Don't finalize.
    
    String storageString = journal.getStorage().toColonSeparatedString();
    System.err.println("storage string: " + storageString);
    journal.close(); // close to unlock the storage dir
    
    // Now re-instantiate, make sure history is still there
    journal = new Journal(TEST_LOG_DIR, mockErrorReporter);
    
    // The storage info should be read, even if no writer has taken over.
    assertEquals(storageString,
        journal.getStorage().toColonSeparatedString());

    assertEquals(1, journal.getLastPromisedEpoch());
    NewEpochResponseProtoOrBuilder newEpoch = journal.newEpoch(FAKE_NSINFO, 2);
    assertEquals(1, newEpoch.getLastSegmentTxId());
  }
  
  @Test
  public void testJournalLocking() throws Exception {
    StorageDirectory sd = journal.getStorage().getStorageDir(0);
    File lockFile = new File(sd.getRoot(), Storage.STORAGE_FILE_LOCK);

    // Journal should not be locked, since we lazily initialize it.
    assertFalse(lockFile.exists());

    journal.newEpoch(FAKE_NSINFO,  1);
    Assume.assumeTrue(journal.getStorage().getStorageDir(0).isLockSupported());
    
    // Journal should be locked
    GenericTestUtils.assertExists(lockFile);
    
    try {
      new Journal(TEST_LOG_DIR, mockErrorReporter);
      fail("Did not fail to create another journal in same dir");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot lock storage", ioe);
    }
    
    journal.close();
    
    // Journal should no longer be locked after the close() call.
    Journal journal2 = new Journal(TEST_LOG_DIR, mockErrorReporter);
    journal2.newEpoch(FAKE_NSINFO, 2);
  }
  
  @Test
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
