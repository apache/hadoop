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
package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.TestEditLog;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

import com.google.common.collect.Lists;

public abstract class QJMTestUtil {
  public static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);
  public static final String JID = "test-journal";

  public static byte[] createTxnData(int startTxn, int numTxns) throws Exception {
    DataOutputBuffer buf = new DataOutputBuffer();
    FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);
    
    for (long txid = startTxn; txid < startTxn + numTxns; txid++) {
      FSEditLogOp op = NameNodeAdapter.createMkdirOp("tx " + txid);
      op.setTransactionId(txid);
      writer.writeOp(op);
    }
    
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  /**
   * Generate byte array representing a set of GarbageMkdirOp
   */
  public static byte[] createGabageTxns(long startTxId, int numTxns)
      throws IOException {
    DataOutputBuffer buf = new DataOutputBuffer();
    FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);

    for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
      FSEditLogOp op = new TestEditLog.GarbageMkdirOp();
      op.setTransactionId(txid);
      writer.writeOp(op);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  public static EditLogOutputStream writeSegment(MiniJournalCluster cluster,
      QuorumJournalManager qjm, long startTxId, int numTxns,
      boolean finalize) throws IOException {
    EditLogOutputStream stm = qjm.startLogSegment(startTxId,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    // Should create in-progress
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(startTxId));
    
    writeTxns(stm, startTxId, numTxns);
    if (finalize) {
      stm.close();
      qjm.finalizeLogSegment(startTxId, startTxId + numTxns - 1);
      return null;
    } else {
      return stm;
    }
  }

  public static void writeOp(EditLogOutputStream stm, long txid) throws IOException {
    FSEditLogOp op = NameNodeAdapter.createMkdirOp("tx " + txid);
    op.setTransactionId(txid);
    stm.write(op);
  }

  public static void writeTxns(EditLogOutputStream stm, long startTxId, int numTxns)
      throws IOException {
    for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
      writeOp(stm, txid);
    }
    stm.setReadyToFlush();
    stm.flush();
  }
  
  /**
   * Verify that the given list of streams contains exactly the range of
   * transactions specified, inclusive.
   */
  public static void verifyEdits(List<EditLogInputStream> streams,
      int firstTxnId, int lastTxnId) throws IOException {
    
    Iterator<EditLogInputStream> iter = streams.iterator();
    assertTrue(iter.hasNext());
    EditLogInputStream stream = iter.next();
    
    for (int expected = firstTxnId;
        expected <= lastTxnId;
        expected++) {
      
      FSEditLogOp op = stream.readOp();
      while (op == null) {
        assertTrue("Expected to find txid " + expected + ", " +
            "but no more streams available to read from",
            iter.hasNext());
        stream = iter.next();
        op = stream.readOp();
      }
      
      assertEquals(FSEditLogOpCodes.OP_MKDIR, op.opCode);
      assertEquals(expected, op.getTransactionId());
    }
    
    assertNull(stream.readOp());
    assertFalse("Expected no more txns after " + lastTxnId +
        " but more streams are available", iter.hasNext());
  }
  

  public static void assertExistsInQuorum(MiniJournalCluster cluster,
      String fname) {
    int count = 0;
    for (int i = 0; i < 3; i++) {
      File dir = cluster.getCurrentDir(i, JID);
      if (new File(dir, fname).exists()) {
        count++;
      }
    }
    assertTrue("File " + fname + " should exist in a quorum of dirs",
        count >= cluster.getQuorumSize());
  }

  public static long recoverAndReturnLastTxn(QuorumJournalManager qjm)
      throws IOException {
    qjm.recoverUnfinalizedSegments();
    long lastRecoveredTxn = 0;

    List<EditLogInputStream> streams = Lists.newArrayList();
    try {
      qjm.selectInputStreams(streams, 0, false);
      
      for (EditLogInputStream elis : streams) {
        assertTrue(elis.getFirstTxId() > lastRecoveredTxn);
        lastRecoveredTxn = elis.getLastTxId();
      }
    } finally {
      IOUtils.cleanup(null, streams.toArray(new Closeable[0]));
    }
    return lastRecoveredTxn;
  }
}
