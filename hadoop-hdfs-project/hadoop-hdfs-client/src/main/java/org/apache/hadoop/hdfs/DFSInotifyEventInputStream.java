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

package org.apache.hadoop.hdfs;

import java.util.Collections;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Stream for reading inotify events. DFSInotifyEventInputStreams should not
 * be shared among multiple threads.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DFSInotifyEventInputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      DFSInotifyEventInputStream.class);

  private final ClientProtocol namenode;
  private Iterator<EventBatch> it;
  private long lastReadTxid;
  /**
   * The most recent txid the NameNode told us it has sync'ed -- helps us
   * determine how far behind we are in the edit stream.
   */
  private long syncTxid;
  /**
   * Used to generate wait times in {@link DFSInotifyEventInputStream#take()}.
   */
  private Random rng = new Random();

  private final Tracer tracer;

  private static final int INITIAL_WAIT_MS = 10;

  DFSInotifyEventInputStream(ClientProtocol namenode, Tracer tracer)
        throws IOException {
    // Only consider new transaction IDs.
    this(namenode, tracer, namenode.getCurrentEditLogTxid());
  }

  DFSInotifyEventInputStream(ClientProtocol namenode, Tracer tracer,
      long lastReadTxid) {
    this.namenode = namenode;
    this.it = Collections.emptyIterator();
    this.lastReadTxid = lastReadTxid;
    this.tracer = tracer;
  }

  /**
   * Returns the next batch of events in the stream or null if no new
   * batches are currently available.
   *
   * @throws IOException because of network error or edit log
   * corruption. Also possible if JournalNodes are unresponsive in the
   * QJM setting (even one unresponsive JournalNode is enough in rare cases),
   * so catching this exception and retrying at least a few times is
   * recommended.
   * @throws MissingEventsException if we cannot return the next batch in the
   * stream because the data for the events (and possibly some subsequent
   * events) has been deleted (generally because this stream is a very large
   * number of transactions behind the current state of the NameNode). It is
   * safe to continue reading from the stream after this exception is thrown
   * The next available batch of events will be returned.
   */
  public EventBatch poll() throws IOException, MissingEventsException {
    try (TraceScope ignored = tracer.newScope("inotifyPoll")) {
      // need to keep retrying until the NN sends us the latest committed txid
      if (lastReadTxid == -1) {
        LOG.debug("poll(): lastReadTxid is -1, reading current txid from NN");
        lastReadTxid = namenode.getCurrentEditLogTxid();
        return null;
      }
      if (!it.hasNext()) {
        EventBatchList el = namenode.getEditsFromTxid(lastReadTxid + 1);
        if (el.getLastTxid() != -1) {
          // we only want to set syncTxid when we were actually able to read some
          // edits on the NN -- otherwise it will seem like edits are being
          // generated faster than we can read them when the problem is really
          // that we are temporarily unable to read edits
          syncTxid = el.getSyncTxid();
          it = el.getBatches().iterator();
          long formerLastReadTxid = lastReadTxid;
          lastReadTxid = el.getLastTxid();
          if (el.getFirstTxid() != formerLastReadTxid + 1) {
            throw new MissingEventsException(formerLastReadTxid + 1,
                el.getFirstTxid());
          }
        } else {
          LOG.debug("poll(): read no edits from the NN when requesting edits " +
              "after txid {}", lastReadTxid);
          return null;
        }
      }

      if (it.hasNext()) { // can be empty if el.getLastTxid != -1 but none of the
        // newly seen edit log ops actually got converted to events
        return it.next();
      } else {
        return null;
      }
    }
  }

  /**
   * Return a estimate of how many transaction IDs behind the NameNode's
   * current state this stream is. Clients should periodically call this method
   * and check if its result is steadily increasing, which indicates that they
   * are falling behind (i.e. transaction are being generated faster than the
   * client is reading them). If a client falls too far behind events may be
   * deleted before the client can read them.
   * <p>
   * A return value of -1 indicates that an estimate could not be produced, and
   * should be ignored. The value returned by this method is really only useful
   * when compared to previous or subsequent returned values.
   */
  public long getTxidsBehindEstimate() {
    if (syncTxid == 0) {
      return -1;
    } else {
      assert syncTxid >= lastReadTxid;
      // this gives the difference between the last txid we have fetched to the
      // client and syncTxid at the time we last fetched events from the
      // NameNode
      return syncTxid - lastReadTxid;
    }
  }

  /**
   * Returns the next event batch in the stream, waiting up to the specified
   * amount of time for a new batch. Returns null if one is not available at the
   * end of the specified amount of time. The time before the method returns may
   * exceed the specified amount of time by up to the time required for an RPC
   * to the NameNode.
   *
   * @param time number of units of the given TimeUnit to wait
   * @param tu the desired TimeUnit
   * @throws IOException see {@link DFSInotifyEventInputStream#poll()}
   * @throws MissingEventsException
   * see {@link DFSInotifyEventInputStream#poll()}
   * @throws InterruptedException if the calling thread is interrupted
   */
  public EventBatch poll(long time, TimeUnit tu) throws IOException,
      InterruptedException, MissingEventsException {
    EventBatch next;
    try (TraceScope ignored = tracer.newScope("inotifyPollWithTimeout")) {
      long initialTime = Time.monotonicNow();
      long totalWait = TimeUnit.MILLISECONDS.convert(time, tu);
      long nextWait = INITIAL_WAIT_MS;
      while ((next = poll()) == null) {
        long timeLeft = totalWait - (Time.monotonicNow() - initialTime);
        if (timeLeft <= 0) {
          LOG.debug("timed poll(): timed out");
          break;
        } else if (timeLeft < nextWait * 2) {
          nextWait = timeLeft;
        } else {
          nextWait *= 2;
        }
        LOG.debug("timed poll(): poll() returned null, sleeping for {} ms",
            nextWait);
        Thread.sleep(nextWait);
      }
    }
    return next;
  }

  /**
   * Returns the next batch of events in the stream, waiting indefinitely if
   * a new batch  is not immediately available.
   *
   * @throws IOException see {@link DFSInotifyEventInputStream#poll()}
   * @throws MissingEventsException see
   * {@link DFSInotifyEventInputStream#poll()}
   * @throws InterruptedException if the calling thread is interrupted
   */
  public EventBatch take() throws IOException, InterruptedException,
      MissingEventsException {
    EventBatch next;
    try (TraceScope ignored = tracer.newScope("inotifyTake")) {
      int nextWaitMin = INITIAL_WAIT_MS;
      while ((next = poll()) == null) {
        // sleep for a random period between nextWaitMin and nextWaitMin * 2
        // to avoid stampedes at the NN if there are multiple clients
        int sleepTime = nextWaitMin + rng.nextInt(nextWaitMin);
        LOG.debug("take(): poll() returned null, sleeping for {} ms", sleepTime);
        Thread.sleep(sleepTime);
        // the maximum sleep is 2 minutes
        nextWaitMin = Math.min(60000, nextWaitMin * 2);
      }
    }

    return next;
  }
}
