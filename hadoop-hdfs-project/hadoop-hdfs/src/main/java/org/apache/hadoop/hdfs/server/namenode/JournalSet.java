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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * Manages a collection of Journals. None of the methods are synchronized, it is
 * assumed that FSEditLog methods, that use this class, use proper
 * synchronization.
 */
public class JournalSet implements JournalManager {

  static final Log LOG = LogFactory.getLog(FSEditLog.class);
  
  /**
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   * 
   * This should be used outside JournalSet only for testing.
   */
  @VisibleForTesting
  static class JournalAndStream {
    private final JournalManager journal;
    private boolean disabled = false;
    private EditLogOutputStream stream;
    
    public JournalAndStream(JournalManager manager) {
      this.journal = manager;
    }

    public void startLogSegment(long txId) throws IOException {
      Preconditions.checkState(stream == null);
      disabled = false;
      stream = journal.startLogSegment(txId);
    }

    /**
     * Closes the stream, also sets it to null.
     */
    public void closeStream() throws IOException {
      if (stream == null) return;
      stream.close();
      stream = null;
    }

    /**
     * Close the Journal and Stream
     */
    public void close() throws IOException {
      closeStream();

      journal.close();
    }
    
    /**
     * Aborts the stream, also sets it to null.
     */
    public void abort() {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
    }

    boolean isActive() {
      return stream != null;
    }
    
    /**
     * Should be used outside JournalSet only for testing.
     */
    EditLogOutputStream getCurrentStream() {
      return stream;
    }
    
    @Override
    public String toString() {
      return "JournalAndStream(mgr=" + journal +
        ", " + "stream=" + stream + ")";
    }

    void setCurrentStreamForTests(EditLogOutputStream stream) {
      this.stream = stream;
    }
    
    JournalManager getManager() {
      return journal;
    }

    private boolean isDisabled() {
      return disabled;
    }

    private void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
  }
  
  private List<JournalAndStream> journals = Lists.newArrayList();
  
  @Override
  public EditLogOutputStream startLogSegment(final long txId) throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId);
      }
    }, "starting log segment " + txId);
    return new JournalSetOutputStream();
  }
  
  @Override
  public void finalizeLogSegment(final long firstTxId, final long lastTxId)
      throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.closeStream();
          jas.getManager().finalizeLogSegment(firstTxId, lastTxId);
        }
      }
    }, "finalize log segment " + firstTxId + ", " + lastTxId);
  }
   
  @Override
  public void close() throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.close();
      }
    }, "close journal");
  }

  
  /**
   * Find the best editlog input stream to read from txid.
   * If a journal throws an CorruptionException while reading from a txn id,
   * it means that it has more transactions, but can't find any from fromTxId. 
   * If this is the case and no other journal has transactions, we should throw
   * an exception as it means more transactions exist, we just can't load them.
   *
   * @param fromTxnId Transaction id to start from.
   * @return A edit log input stream with tranactions fromTxId 
   *         or null if no more exist
   */
  @Override
  public EditLogInputStream getInputStream(long fromTxnId) throws IOException {
    JournalManager bestjm = null;
    long bestjmNumTxns = 0;
    CorruptionException corruption = null;

    for (JournalAndStream jas : journals) {
      JournalManager candidate = jas.getManager();
      long candidateNumTxns = 0;
      try {
        candidateNumTxns = candidate.getNumberOfTransactions(fromTxnId);
      } catch (CorruptionException ce) {
        corruption = ce;
      } catch (IOException ioe) {
        continue; // error reading disk, just skip
      }
      
      if (candidateNumTxns > bestjmNumTxns) {
        bestjm = candidate;
        bestjmNumTxns = candidateNumTxns;
      }
    }
    
    if (bestjm == null) {
      if (corruption != null) {
        throw new IOException("No non-corrupt logs for txid " 
                                        + fromTxnId, corruption);
      } else {
        return null;
      }
    }
    return bestjm.getInputStream(fromTxnId);
  }
  
  @Override
  public long getNumberOfTransactions(long fromTxnId) throws IOException {
    long num = 0;
    for (JournalAndStream jas: journals) {
      if (jas.isActive()) {
        long newNum = jas.getManager().getNumberOfTransactions(fromTxnId);
        if (newNum > num) {
          num = newNum;
        }
      }
    }
    return num;
  }

  /**
   * Returns true if there are no journals or all are disabled.
   * @return True if no journals or all are disabled.
   */
  public boolean isEmpty() {
    for (JournalAndStream jas : journals) {
      if (!jas.isDisabled()) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Called when some journals experience an error in some operation.
   */
  private void disableAndReportErrorOnJournals(List<JournalAndStream> badJournals) {
    if (badJournals == null || badJournals.isEmpty()) {
      return; // nothing to do
    }
 
    for (JournalAndStream j : badJournals) {
      LOG.error("Disabling journal " + j);
      j.abort();
      j.setDisabled(true);
    }
  }

  /**
   * Implementations of this interface encapsulate operations that can be
   * iteratively applied on all the journals. For example see
   * {@link JournalSet#mapJournalsAndReportErrors}.
   */
  private interface JournalClosure {
    /**
     * The operation on JournalAndStream.
     * @param jas Object on which operations are performed.
     * @throws IOException
     */
    public void apply(JournalAndStream jas) throws IOException;
  }
  
  /**
   * Apply the given operation across all of the journal managers, disabling
   * any for which the closure throws an IOException.
   * @param closure {@link JournalClosure} object encapsulating the operation.
   * @param status message used for logging errors (e.g. "opening journal")
   * @throws IOException If the operation fails on all the journals.
   */
  private void mapJournalsAndReportErrors(
      JournalClosure closure, String status) throws IOException{
    List<JournalAndStream> badJAS = Lists.newLinkedList();
    for (JournalAndStream jas : journals) {
      try {
        closure.apply(jas);
      } catch (Throwable t) {
        LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
        badJAS.add(jas);
      }
    }
    disableAndReportErrorOnJournals(badJAS);
    if (badJAS.size() >= journals.size()) {
      LOG.error("Error: "+status+" failed for all journals");
      throw new IOException(status+" failed on all the journals");
    }
  }
  
  /**
   * An implementation of EditLogOutputStream that applies a requested method on
   * all the journals that are currently active.
   */
  private class JournalSetOutputStream extends EditLogOutputStream {

    JournalSetOutputStream() throws IOException {
      super();
    }

    @Override
    void write(final FSEditLogOp op)
        throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().write(op);
          }
        }
      }, "write op");
    }

    @Override
    void writeRaw(final byte[] data, final int offset, final int length)
        throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().writeRaw(data, offset, length);
          }
        }
      }, "write bytes");
    }

    @Override
    void create() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().create();
          }
        }
      }, "create");
    }

    @Override
    public void close() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.closeStream();
        }
      }, "close");
    }

    @Override
    public void abort() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.abort();
        }
      }, "abort");
    }

    @Override
    void setReadyToFlush() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().setReadyToFlush();
          }
        }
      }, "setReadyToFlush");
    }

    @Override
    protected void flushAndSync() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flushAndSync();
          }
        }
      }, "flushAndSync");
    }
    
    @Override
    public void flush() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flush();
          }
        }
      }, "flush");
    }
    
    @Override
    public boolean shouldForceSync() {
      for (JournalAndStream js : journals) {
        if (js.isActive() && js.getCurrentStream().shouldForceSync()) {
          return true;
        }
      }
      return false;
    }
    
    @Override
    protected long getNumSync() {
      for (JournalAndStream jas : journals) {
        if (jas.isActive()) {
          return jas.getCurrentStream().getNumSync();
        }
      }
      return 0;
    }
  }

  @Override
  public void setOutputBufferCapacity(final int size) {
    try {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
            jas.getManager().setOutputBufferCapacity(size);
        }
      }, "setOutputBufferCapacity");
    } catch (IOException e) {
      LOG.error("Error in setting outputbuffer capacity");
    }
  }
  
  @VisibleForTesting
  List<JournalAndStream> getAllJournalStreams() {
    return journals;
  }

  List<JournalManager> getJournalManagers() {
    List<JournalManager> jList = new ArrayList<JournalManager>();
    for (JournalAndStream j : journals) {
      jList.add(j.getManager());
    }
    return jList;
  }

  void add(JournalManager j) {
    journals.add(new JournalAndStream(j));
  }
  
  void remove(JournalManager j) {
    JournalAndStream jasToRemove = null;
    for (JournalAndStream jas: journals) {
      if (jas.getManager().equals(j)) {
        jasToRemove = jas;
        break;
      }
    }
    if (jasToRemove != null) {
      jasToRemove.abort();
      journals.remove(jasToRemove);
    }
  }

  @Override
  public void purgeLogsOlderThan(final long minTxIdToKeep) throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().purgeLogsOlderThan(minTxIdToKeep);
      }
    }, "purgeLogsOlderThan " + minTxIdToKeep);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().recoverUnfinalizedSegments();
      }
    }, "recoverUnfinalizedSegments");
  }
  
  /**
   * Return a manifest of what finalized edit logs are available. All available
   * edit logs are returned starting from the transaction id passed.
   * 
   * @param fromTxId Starting transaction id to read the logs.
   * @return RemoteEditLogManifest object.
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId) {
    // Collect RemoteEditLogs available from each FileJournalManager
    List<RemoteEditLog> allLogs = Lists.newArrayList();
    for (JournalAndStream j : journals) {
      if (j.getManager() instanceof FileJournalManager) {
        FileJournalManager fjm = (FileJournalManager)j.getManager();
        try {
          allLogs.addAll(fjm.getRemoteEditLogs(fromTxId));
        } catch (Throwable t) {
          LOG.warn("Cannot list edit logs in " + fjm, t);
        }
      }
    }
    
    // Group logs by their starting txid
    ImmutableListMultimap<Long, RemoteEditLog> logsByStartTxId =
      Multimaps.index(allLogs, RemoteEditLog.GET_START_TXID);
    long curStartTxId = fromTxId;

    List<RemoteEditLog> logs = Lists.newArrayList();
    while (true) {
      ImmutableList<RemoteEditLog> logGroup = logsByStartTxId.get(curStartTxId);
      if (logGroup.isEmpty()) {
        // we have a gap in logs - for example because we recovered some old
        // storage directory with ancient logs. Clear out any logs we've
        // accumulated so far, and then skip to the next segment of logs
        // after the gap.
        SortedSet<Long> startTxIds = Sets.newTreeSet(logsByStartTxId.keySet());
        startTxIds = startTxIds.tailSet(curStartTxId);
        if (startTxIds.isEmpty()) {
          break;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found gap in logs at " + curStartTxId + ": " +
                "not returning previous logs in manifest.");
          }
          logs.clear();
          curStartTxId = startTxIds.first();
          continue;
        }
      }

      // Find the one that extends the farthest forward
      RemoteEditLog bestLog = Collections.max(logGroup);
      logs.add(bestLog);
      // And then start looking from after that point
      curStartTxId = bestLog.getEndTxId() + 1;
    }
    RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated manifest for logs since " + fromTxId + ":"
          + ret);      
    }
    return ret;
  }

  /**
   * Add sync times to the buffer.
   */
  String getSyncTimes() {
    StringBuilder buf = new StringBuilder();
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        buf.append(jas.getCurrentStream().getTotalSyncTime());
        buf.append(" ");
      }
    }
    return buf.toString();
  }
}
