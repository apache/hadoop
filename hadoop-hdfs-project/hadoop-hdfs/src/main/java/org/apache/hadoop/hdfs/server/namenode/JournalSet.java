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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

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
@InterfaceAudience.Private
public class JournalSet implements JournalManager {

  static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

  // we want local logs to be ordered earlier in the collection, and true
  // is considered larger than false, so reverse the comparator
  private static final Comparator<EditLogInputStream>
      LOCAL_LOG_PREFERENCE_COMPARATOR = Comparator
      .comparing(EditLogInputStream::isLocalLog)
      .reversed();

  public static final Comparator<EditLogInputStream>
      EDIT_LOG_INPUT_STREAM_COMPARATOR = Comparator
      .comparing(EditLogInputStream::getFirstTxId)
      .thenComparing(EditLogInputStream::getLastTxId);

  /**
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   */
  static class JournalAndStream implements CheckableNameNodeResource {
    private final JournalManager journal;
    private boolean disabled = false;
    private EditLogOutputStream stream;
    private final boolean required;
    private final boolean shared;
    
    public JournalAndStream(JournalManager manager, boolean required,
        boolean shared) {
      this.journal = manager;
      this.required = required;
      this.shared = shared;
    }

    public void startLogSegment(long txId, int layoutVersion) throws IOException {
      Preconditions.checkState(stream == null);
      disabled = false;
      stream = journal.startLogSegment(txId, layoutVersion);
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

    boolean isDisabled() {
      return disabled;
    }

    private void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
    
    @Override
    public boolean isResourceAvailable() {
      return !isDisabled();
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }
    
    public boolean isShared() {
      return shared;
    }
  }
 
  // COW implementation is necessary since some users (eg the web ui) call
  // getAllJournalStreams() and then iterate. Since this is rarely
  // mutated, there is no performance concern.
  private final List<JournalAndStream> journals =
      new CopyOnWriteArrayList<JournalSet.JournalAndStream>();
  final int minimumRedundantJournals;

  private boolean closed;
  
  JournalSet(int minimumRedundantResources) {
    this.minimumRedundantJournals = minimumRedundantResources;
  }
  
  @Override
  public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    // The operation is done by FSEditLog itself
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasSomeData() throws IOException {
    // This is called individually on the underlying journals,
    // not on the JournalSet.
    throw new UnsupportedOperationException();
  }

  
  @Override
  public EditLogOutputStream startLogSegment(final long txId,
      final int layoutVersion) throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId, layoutVersion);
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
    closed = true;
  }

  public boolean isOpen() {
    return !closed;
  }

  /**
   * In this function, we get a bunch of streams from all of our JournalManager
   * objects.  Then we add these to the collection one by one.
   * 
   * @param streams          The collection to add the streams to.  It may or 
   *                         may not be sorted-- this is up to the caller.
   * @param fromTxId         The transaction ID to start looking for streams at
   * @param inProgressOk     Should we consider unfinalized streams?
   * @param onlyDurableTxns  Set to true if streams are bounded by the durable
   *                         TxId. A durable TxId is the committed txid in QJM
   *                         or the largest txid written into file in FJM
   */
  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk, boolean onlyDurableTxns) {
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (JournalAndStream jas : journals) {
      if (jas.isDisabled()) {
        LOG.info("Skipping jas " + jas + " since it's disabled");
        continue;
      }
      try {
        jas.getManager().selectInputStreams(allStreams, fromTxId,
            inProgressOk, onlyDurableTxns);
      } catch (IOException ioe) {
        LOG.warn("Unable to determine input streams from " + jas.getManager() +
            ". Skipping.", ioe);
      }
    }
    chainAndMakeRedundantStreams(streams, allStreams, fromTxId);
  }
  
  public static void chainAndMakeRedundantStreams(
      Collection<EditLogInputStream> outStreams,
      PriorityQueue<EditLogInputStream> allStreams, long fromTxId) {
    // We want to group together all the streams that start on the same start
    // transaction ID.  To do this, we maintain an accumulator (acc) of all
    // the streams we've seen at a given start transaction ID.  When we see a
    // higher start transaction ID, we select a stream from the accumulator and
    // clear it.  Then we begin accumulating streams with the new, higher start
    // transaction ID.
    LinkedList<EditLogInputStream> acc =
        new LinkedList<EditLogInputStream>();
    EditLogInputStream elis;
    while ((elis = allStreams.poll()) != null) {
      if (acc.isEmpty()) {
        acc.add(elis);
      } else {
        EditLogInputStream accFirst = acc.get(0);
        long accFirstTxId = accFirst.getFirstTxId();
        if (accFirstTxId == elis.getFirstTxId()) {
          // if we have a finalized log segment available at this txid,
          // we should throw out all in-progress segments at this txid
          if (elis.isInProgress()) {
            if (accFirst.isInProgress()) {
              acc.add(elis);
            }
          } else {
            if (accFirst.isInProgress()) {
              acc.clear();
            }
            acc.add(elis);
          }
        } else if (accFirstTxId < elis.getFirstTxId()) {
          // try to read from the local logs first since the throughput should
          // be higher
          Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
          outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
          acc.clear();
          acc.add(elis);
        } else if (accFirstTxId > elis.getFirstTxId()) {
          throw new RuntimeException("sorted set invariants violated!  " +
              "Got stream with first txid " + elis.getFirstTxId() +
              ", but the last firstTxId was " + accFirstTxId);
        }
      }
    }
    if (!acc.isEmpty()) {
      Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
      outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
      acc.clear();
    }
  }

  /**
   * Returns true if there are no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   * 
   * @return True if there no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   */
  public boolean isEmpty() {
    return !NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals);
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
        if (jas.isRequired()) {
          final String msg = "Error: " + status + " failed for required journal ("
            + jas + ")";
          LOG.error(msg, t);
          // If we fail on *any* of the required journals, then we must not
          // continue on any of the other journals. Abort them to ensure that
          // retry behavior doesn't allow them to keep going in any way.
          abortAllJournals();
          // the current policy is to shutdown the NN on errors to shared edits
          // dir. There are many code paths to shared edits failures - syncs,
          // roll of edits etc. All of them go through this common function 
          // where the isRequired() check is made. Applying exit policy here 
          // to catch all code paths.
          terminate(1, msg);
        } else {
          LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
          badJAS.add(jas);          
        }
      }
    }
    disableAndReportErrorOnJournals(badJAS);
    if (!NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals)) {
      String message = status + " failed for too many journals";
      LOG.error("Error: " + message);
      throw new IOException(message);
    }
  }
  
  /**
   * Abort all of the underlying streams.
   */
  private void abortAllJournals() {
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        jas.abort();
      }
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
    public void write(final FSEditLogOp op)
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
    public void writeRaw(final byte[] data, final int offset, final int length)
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
    public void create(final int layoutVersion) throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().create(layoutVersion);
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
    public void setReadyToFlush() throws IOException {
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
    protected void flushAndSync(final boolean durable) throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flushAndSync(durable);
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
  
  void add(JournalManager j, boolean required) {
    add(j, required, false);
  }
  
  void add(JournalManager j, boolean required, boolean shared) {
    JournalAndStream jas = new JournalAndStream(j, required, shared);
    journals.add(jas);
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
   * edit logs are returned starting from the transaction id passed. If
   * 'fromTxId' falls in the middle of a log, that log is returned as well.
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
          allLogs.addAll(fjm.getRemoteEditLogs(fromTxId, false));
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
    RemoteEditLogManifest ret = new RemoteEditLogManifest(logs,
        curStartTxId - 1);
    
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

  @Override
  public void doPreUpgrade() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void doFinalize() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage, int targetLayoutVersion) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void doRollback() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public long getJournalCTime() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }
}
