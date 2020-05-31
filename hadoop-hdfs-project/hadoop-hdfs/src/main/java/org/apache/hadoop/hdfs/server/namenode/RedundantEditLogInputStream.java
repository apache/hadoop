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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;

/**
 * A merged input stream that handles failover between different edit logs.
 *
 * We will currently try each edit log stream exactly once.  In other words, we
 * don't handle the "ping pong" scenario where different edit logs contain a
 * different subset of the available edits.
 */
class RedundantEditLogInputStream extends EditLogInputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      RedundantEditLogInputStream.class.getName());
  private int curIdx;
  private long prevTxId;
  private final EditLogInputStream[] streams;

  /** Limit logging about fast forwarding the stream to every 5 seconds max. */
  private static final long FAST_FORWARD_LOGGING_INTERVAL_MS = 5000;
  private final LogThrottlingHelper fastForwardLoggingHelper =
      new LogThrottlingHelper(FAST_FORWARD_LOGGING_INTERVAL_MS);

  /**
   * States that the RedundantEditLogInputStream can be in.
   *
   * <pre>
   *                   start (if no streams)
   *                           |
   *                           V
   * PrematureEOFException  +----------------+
   *        +-------------->| EOF            |<--------------+
   *        |               +----------------+               |
   *        |                                                |
   *        |          start (if there are streams)          |
   *        |                  |                             |
   *        |                  V                             | EOF
   *        |   resync      +----------------+ skipUntil  +---------+
   *        |   +---------->| SKIP_UNTIL     |----------->|  OK     |
   *        |   |           +----------------+            +---------+
   *        |   |                | IOE   ^ fail over to      | IOE
   *        |   |                V       | next stream       |
   * +----------------------+   +----------------+           |
   * | STREAM_FAILED_RESYNC |   | STREAM_FAILED  |<----------+
   * +----------------------+   +----------------+
   *                  ^   Recovery mode    |
   *                  +--------------------+
   * </pre>
   */
  static private enum State {
    /** We need to skip until prevTxId + 1 */
    SKIP_UNTIL,
    /** We're ready to read opcodes out of the current stream */
    OK,
    /** The current stream has failed. */
    STREAM_FAILED,
    /** The current stream has failed, and resync() was called.  */
    STREAM_FAILED_RESYNC,
    /** There are no more opcodes to read from this
     * RedundantEditLogInputStream */
    EOF;
  }

  private State state;
  private IOException prevException;

  RedundantEditLogInputStream(Collection<EditLogInputStream> streams,
      long startTxId) {
    this.curIdx = 0;
    this.prevTxId = (startTxId == HdfsServerConstants.INVALID_TXID) ?
      HdfsServerConstants.INVALID_TXID : (startTxId - 1);
    this.state = (streams.isEmpty()) ? State.EOF : State.SKIP_UNTIL;
    this.prevException = null;
    // EditLogInputStreams in a RedundantEditLogInputStream must be finalized,
    // and can't be pre-transactional.
    EditLogInputStream first = null;
    for (EditLogInputStream s : streams) {
      Preconditions.checkArgument(s.getFirstTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid first txid in stream: %s", s);
      Preconditions.checkArgument(s.getLastTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid last txid in stream: %s", s);
      if (first == null) {
        first = s;
      } else {
        Preconditions.checkArgument(s.getFirstTxId() == first.getFirstTxId(),
          "All streams in the RedundantEditLogInputStream must have the same " +
          "start transaction ID!  " + first + " had start txId " +
          first.getFirstTxId() + ", but " + s + " had start txId " +
          s.getFirstTxId());
      }
    }

    this.streams = streams.toArray(new EditLogInputStream[0]);

    // We sort the streams here so that the streams that end later come first.
    Arrays.sort(this.streams, new Comparator<EditLogInputStream>() {
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return Longs.compare(b.getLastTxId(), a.getLastTxId());
      }
    });
  }

  @Override
  public String getCurrentStreamName() {
    return streams[curIdx].getCurrentStreamName();
  }

  @Override
  public String getName() {
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (EditLogInputStream elis : streams) {
      bld.append(prefix)
          .append(elis.getName());
      prefix = ", ";
    }
    return bld.toString();
  }

  @Override
  public long getFirstTxId() {
    return streams[curIdx].getFirstTxId();
  }

  @Override
  public long getLastTxId() {
    return streams[curIdx].getLastTxId();
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG,  streams);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      if (state == State.STREAM_FAILED) {
        state = State.STREAM_FAILED_RESYNC;
      }
      return nextOp();
    } catch (IOException e) {
      LOG.warn("encountered an exception", e);
      return null;
    }
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    while (true) {
      switch (state) {
      case SKIP_UNTIL:
       try {
          if (prevTxId != HdfsServerConstants.INVALID_TXID) {
            LogAction logAction = fastForwardLoggingHelper.record();
            if (logAction.shouldLog()) {
              LOG.info("Fast-forwarding stream '" + streams[curIdx].getName() +
                  "' to transaction ID " + (prevTxId + 1) +
                  LogThrottlingHelper.getLogSupressionMessage(logAction));
            }
            streams[curIdx].skipUntil(prevTxId + 1);
          }
        } catch (IOException e) {
          prevException = e;
          state = State.STREAM_FAILED;
        }
        state = State.OK;
        break;
      case OK:
        try {
          FSEditLogOp op = streams[curIdx].readOp();
          if (op == null) {
            state = State.EOF;
            if (streams[curIdx].getLastTxId() == prevTxId) {
              return null;
            } else {
              throw new PrematureEOFException("got premature end-of-file " +
                  "at txid " + prevTxId + "; expected file to go up to " +
                  streams[curIdx].getLastTxId());
            }
          }
          prevTxId = op.getTransactionId();
          return op;
        } catch (IOException e) {
          prevException = e;
          state = State.STREAM_FAILED;
        }
        break;
      case STREAM_FAILED:
        if (curIdx + 1 == streams.length) {
          throw prevException;
        }
        long oldLast = streams[curIdx].getLastTxId();
        long newLast = streams[curIdx + 1].getLastTxId();
        if (newLast < oldLast) {
          throw new IOException("We encountered an error reading " +
              streams[curIdx].getName() + ".  During automatic edit log " +
              "failover, we noticed that all of the remaining edit log " +
              "streams are shorter than the current one!  The best " +
              "remaining edit log ends at transaction " +
              newLast + ", but we thought we could read up to transaction " +
              oldLast + ".  If you continue, metadata will be lost forever!",
              prevException);
        }
        LOG.error("Got error reading edit log input stream " +
          streams[curIdx].getName() + "; failing over to edit log " +
          streams[curIdx + 1].getName(), prevException);
        curIdx++;
        state = State.SKIP_UNTIL;
        break;
      case STREAM_FAILED_RESYNC:
        if (curIdx + 1 == streams.length) {
          if (prevException instanceof PrematureEOFException) {
            // bypass early EOF check
            state = State.EOF;
          } else {
            streams[curIdx].resync();
            state = State.SKIP_UNTIL;
          }
        } else {
          LOG.error("failing over to edit log " +
              streams[curIdx + 1].getName());
          curIdx++;
          state = State.SKIP_UNTIL;
        }
        break;
      case EOF:
        return null;
      }
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    return streams[curIdx].getVersion(verifyVersion);
  }

  @Override
  public long getPosition() {
    return streams[curIdx].getPosition();
  }

  @Override
  public long length() throws IOException {
    return streams[curIdx].length();
  }

  @Override
  public boolean isInProgress() {
    return streams[curIdx].isInProgress();
  }

  static private final class PrematureEOFException extends IOException {
    private static final long serialVersionUID = 1L;
    PrematureEOFException(String msg) {
      super(msg);
    }
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    for (EditLogInputStream elis : streams) {
      elis.setMaxOpSize(maxOpSize);
    }
  }

  @Override
  public boolean isLocalLog() {
    return streams[curIdx].isLocalLog();
  }
}
