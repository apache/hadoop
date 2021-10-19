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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * A double-buffer for edits. New edits are written into the first buffer
 * while the second is available to be flushed. Each time the double-buffer
 * is flushed, the two internal buffers are swapped. This allows edits
 * to progress concurrently to flushes without allocating new buffers each
 * time.
 */
@InterfaceAudience.Private
public class EditsDoubleBuffer {
  protected static final Logger LOG =
      LoggerFactory.getLogger(EditsDoubleBuffer.class);

  private TxnBuffer bufCurrent; // current buffer for writing
  private TxnBuffer bufReady; // buffer ready for flushing
  private final int initBufferSize;

  public EditsDoubleBuffer(int defaultBufferSize) {
    initBufferSize = defaultBufferSize;
    bufCurrent = new TxnBuffer(initBufferSize);
    bufReady = new TxnBuffer(initBufferSize);

  }

  public void writeOp(FSEditLogOp op, int logVersion) throws IOException {
    bufCurrent.writeOp(op, logVersion);
  }

  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    bufCurrent.write(bytes, offset, length);
  }
  
  public void close() throws IOException {
    Preconditions.checkNotNull(bufCurrent);
    Preconditions.checkNotNull(bufReady);

    int bufSize = bufCurrent.size();
    if (bufSize != 0) {
      bufCurrent.dumpRemainingEditLogs();
      throw new IOException("FSEditStream has " + bufSize
          + " bytes still to be flushed and cannot be closed.");
    }

    IOUtils.cleanupWithLogger(null, bufCurrent, bufReady);
    bufCurrent = bufReady = null;
  }
  
  public void setReadyToFlush() {
    assert isFlushed() : "previous data not flushed yet";
    TxnBuffer tmp = bufReady;
    bufReady = bufCurrent;
    bufCurrent = tmp;
  }
  
  /**
   * Writes the content of the "ready" buffer to the given output stream,
   * and resets it. Does not swap any buffers.
   */
  public void flushTo(OutputStream out) throws IOException {
    bufReady.writeTo(out); // write data to file
    bufReady.reset(); // erase all data in the buffer
  }
  
  public boolean shouldForceSync() {
    return bufCurrent.size() >= initBufferSize;
  }

  DataOutputBuffer getReadyBuf() {
    return bufReady;
  }
  
  DataOutputBuffer getCurrentBuf() {
    return bufCurrent;
  }

  public boolean isFlushed() {
    return bufReady.size() == 0;
  }

  public int countBufferedBytes() {
    return bufReady.size() + bufCurrent.size();
  }

  /**
   * @return the transaction ID of the first transaction ready to be flushed 
   */
  public long getFirstReadyTxId() {
    assert bufReady.firstTxId > 0;
    return bufReady.firstTxId;
  }

  /**
   * @return the number of transactions that are ready to be flushed
   */
  public int countReadyTxns() {
    return bufReady.numTxns;
  }

  /**
   * @return the number of bytes that are ready to be flushed
   */
  public int countReadyBytes() {
    return bufReady.size();
  }
  
  private static class TxnBuffer extends DataOutputBuffer {
    long firstTxId;
    int numTxns;
    private final Writer writer;
    
    public TxnBuffer(int initBufferSize) {
      super(initBufferSize);
      writer = new FSEditLogOp.Writer(this);
      reset();
    }

    public void writeOp(FSEditLogOp op, int logVersion) throws IOException {
      if (firstTxId == HdfsServerConstants.INVALID_TXID) {
        firstTxId = op.txid;
      } else {
        assert op.txid > firstTxId;
      }
      writer.writeOp(op, logVersion);
      numTxns++;
    }
    
    @Override
    public DataOutputBuffer reset() {
      super.reset();
      firstTxId = HdfsServerConstants.INVALID_TXID;
      numTxns = 0;
      return this;
    }

    private void dumpRemainingEditLogs() {
      byte[] buf = this.getData();
      byte[] remainingRawEdits = Arrays.copyOfRange(buf, 0, this.size());
      ByteArrayInputStream bis = new ByteArrayInputStream(remainingRawEdits);
      DataInputStream dis = new DataInputStream(bis);
      FSEditLogLoader.PositionTrackingInputStream tracker =
          new FSEditLogLoader.PositionTrackingInputStream(bis);
      FSEditLogOp.Reader reader = FSEditLogOp.Reader.create(dis, tracker,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      FSEditLogOp op;
      LOG.warn("The edits buffer is " + size() + " bytes long with " + numTxns +
          " unflushed transactions. " +
          "Below is the list of unflushed transactions:");
      int numTransactions = 0;
      try {
        while ((op = reader.readOp(false)) != null) {
          LOG.warn("Unflushed op [" + numTransactions + "]: " + op);
          numTransactions++;
        }
      } catch (IOException ioe) {
        // If any exceptions, print raw bytes and stop.
        LOG.warn("Unable to dump remaining ops. Remaining raw bytes: " +
            Hex.encodeHexString(remainingRawEdits), ioe);
      }
    }
  }

}
