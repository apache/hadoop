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
package org.apache.hadoop.contrib.bkjournal;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Input stream which reads from a BookKeeper ledger.
 */
class BookKeeperEditLogInputStream extends EditLogInputStream {
  static final Log LOG = LogFactory.getLog(BookKeeperEditLogInputStream.class);

  private final long firstTxId;
  private final long lastTxId;
  private final int logVersion;
  private final LedgerHandle lh;

  private final FSEditLogOp.Reader reader;
  private final FSEditLogLoader.PositionTrackingInputStream tracker;

  /**
   * Construct BookKeeper edit log input stream.
   * Starts reading from the first entry of the ledger.
   */
  BookKeeperEditLogInputStream(final LedgerHandle lh, 
                               final EditLogLedgerMetadata metadata)
      throws IOException {
    this(lh, metadata, 0);
  }

  /**
   * Construct BookKeeper edit log input stream. 
   * Starts reading from firstBookKeeperEntry. This allows the stream
   * to take a shortcut during recovery, as it doesn't have to read
   * every edit log transaction to find out what the last one is.
   */
  BookKeeperEditLogInputStream(LedgerHandle lh, EditLogLedgerMetadata metadata,
                               long firstBookKeeperEntry) 
      throws IOException {
    this.lh = lh;
    this.firstTxId = metadata.getFirstTxId();
    this.lastTxId = metadata.getLastTxId();
    this.logVersion = metadata.getVersion();

    BufferedInputStream bin = new BufferedInputStream(
        new LedgerInputStream(lh, firstBookKeeperEntry));
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
    DataInputStream in = new DataInputStream(tracker);

    reader = new FSEditLogOp.Reader(in, logVersion);
  }

  @Override
  public long getFirstTxId() throws IOException {
    return firstTxId;
  }

  @Override
  public long getLastTxId() throws IOException {
    return lastTxId;
  }
  
  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    return reader.readOp(false);
  }

  @Override
  public void close() throws IOException {
    try {
      lh.close();
    } catch (Exception e) {
      throw new IOException("Exception closing ledger", e);
    }
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public long length() throws IOException {
    return lh.getLength();
  }
  
  @Override
  public String getName() {
    return String.format("BookKeeper[%s,first=%d,last=%d]", 
        lh.toString(), firstTxId, lastTxId);
  }

  // TODO(HA): Test this.
  @Override
  public boolean isInProgress() {
    return true;
  }

  /**
   * Input stream implementation which can be used by 
   * FSEditLogOp.Reader
   */
  private static class LedgerInputStream extends InputStream {
    private long readEntries;
    private InputStream entryStream = null;
    private final LedgerHandle lh;
    private final long maxEntry;

    /**
     * Construct ledger input stream
     * @param lh the ledger handle to read from
     * @param firstBookKeeperEntry ledger entry to start reading from
     */
    LedgerInputStream(LedgerHandle lh, long firstBookKeeperEntry) 
        throws IOException {
      this.lh = lh;
      readEntries = firstBookKeeperEntry;
      try {
        maxEntry = lh.getLastAddConfirmed();
      } catch (Exception e) {
        throw new IOException("Error reading last entry id", e);
      }
    }

    /**
     * Get input stream representing next entry in the
     * ledger.
     * @return input stream, or null if no more entries
     */
    private InputStream nextStream() throws IOException {
      try {        
        if (readEntries > maxEntry) {
          return null;
        }
        Enumeration<LedgerEntry> entries 
          = lh.readEntries(readEntries, readEntries);
        readEntries++;
        if (entries.hasMoreElements()) {
            LedgerEntry e = entries.nextElement();
            assert !entries.hasMoreElements();
            return e.getEntryInputStream();
        }
      } catch (Exception e) {
        throw new IOException("Error reading entries from bookkeeper", e);
      }
      return null;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      if (read(b, 0, 1) != 1) {
        return -1;
      } else {
        return b[0];
      }
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        int read = 0;
        if (entryStream == null) {
          entryStream = nextStream();
          if (entryStream == null) {
            return read;
          }
        }

        while (read < len) {
          int thisread = entryStream.read(b, off+read, (len-read));
          if (thisread == -1) {
            entryStream = nextStream();
            if (entryStream == null) {
              return read;
            }
          } else {
            read += thisread;
          }
        }
        return read;
      } catch (IOException e) {
        throw e;
      }

    }
  }
}
