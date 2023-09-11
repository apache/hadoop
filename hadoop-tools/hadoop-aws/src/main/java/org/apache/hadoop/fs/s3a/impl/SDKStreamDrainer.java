/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.http.Abortable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.util.functional.CallableRaisingIOE;


import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DRAIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration;

/**
 * Drains/aborts s3 or other AWS SDK streams.
 * It is callable so can be passed directly to a submitter
 * for async invocation.
 */
public class SDKStreamDrainer<TStream extends InputStream & Abortable>
    implements CallableRaisingIOE<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SDKStreamDrainer.class);

  /**
   * URI for log messages.
   */
  private final String uri;

  /**
   * Stream from the getObject response for draining and closing.
   */
  private final TStream sdkStream;

  /**
   * Should the request be aborted?
   */
  private final boolean shouldAbort;

  /**
   * How many bytes remaining?
   * This is decremented as the stream is
   * drained;
   * If the stream finished before the expected
   * remaining value was read, this will show how many
   * bytes were still expected.
   */
  private int remaining;

  /**
   * Statistics to update with the duration.
   */
  private final S3AInputStreamStatistics streamStatistics;

  /**
   * Reason? for log messages.
   */
  private final String reason;

  /**
   * Has the operation executed yet?
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Any exception caught during execution.
   */
  private Exception thrown;

  /**
   * Was the stream aborted?
   */
  private boolean aborted;

  /**
   * how many bytes were drained?
   */
  private int drained = 0;

  /**
   * Prepare to drain the stream.
   * @param uri URI for messages
   * @param sdkStream stream to close.
   * @param shouldAbort force an abort; used if explicitly requested.
   * @param streamStatistics stats to update
   * @param reason reason for stream being closed; used in messages
   * @param remaining remaining bytes
   */
  public SDKStreamDrainer(final String uri,
      final TStream sdkStream,
      final boolean shouldAbort,
      final int remaining,
      final S3AInputStreamStatistics streamStatistics,
      final String reason) {
    this.uri = uri;
    this.sdkStream = requireNonNull(sdkStream);
    this.shouldAbort = shouldAbort;
    this.remaining = remaining;
    this.streamStatistics = requireNonNull(streamStatistics);
    this.reason = reason;
  }

  /**
   * drain the stream. This method is intended to be
   * used directly or asynchronously, and measures the
   * duration of the operation in the stream statistics.
   * @return was the stream aborted?
   */
  @Override
  public Boolean apply() {
    try {
      Boolean outcome = invokeTrackingDuration(
          streamStatistics.initiateInnerStreamClose(shouldAbort),
          this::drainOrAbortHttpStream);
      aborted = outcome;
      return outcome;
    } catch (Exception e) {
      thrown = e;
      return aborted;
    }
  }

  /**
   * Apply, raising any exception.
   * For testing.
   * @return the outcome.
   * @throws Exception anything raised.
   */
  @VisibleForTesting
  boolean applyRaisingException() throws Exception {
    Boolean outcome = apply();
    if (thrown != null) {
      throw thrown;
    }
    return outcome;
  }

  /**
   * Drain or abort the inner stream.
   * Exceptions are saved then swallowed.
   * If a close() is attempted and fails, the operation escalates to
   * an abort.
   * @return true if the stream was aborted.
   */
  private boolean drainOrAbortHttpStream() {
    if (executed.getAndSet(true)) {
      throw new IllegalStateException(
          "duplicate invocation of drain operation");
    }
    boolean executeAbort = shouldAbort;
    LOG.debug("drain or abort reason {} remaining={} abort={}",
        reason, remaining, executeAbort);

    if (!executeAbort) {
      try {
        // clean close. This will read to the end of the stream,
        // so, while cleaner, can be pathological on a multi-GB object

        if (remaining > 0) {
          // explicitly drain the stream
          LOG.debug("draining {} bytes", remaining);
          drained = 0;
          int size = Math.min(remaining, DRAIN_BUFFER_SIZE);
          byte[] buffer = new byte[size];
          // read the data; bail out early if
          // the connection breaks.
          // this may be a bit overaggressive on buffer underflow.
          while (remaining > 0) {
            final int count = sdkStream.read(buffer);
            LOG.debug("read {} bytes", count);
            if (count <= 0) {
              // no more data is left
              break;
            }
            drained += count;
            remaining -= count;
          }
          LOG.debug("Drained stream of {} bytes", drained);
        }

        if (remaining != 0) {
          // fewer bytes than expected came back; not treating as a
          // reason to escalate to an abort().
          // just log.
          LOG.debug("drained fewer bytes than expected; {} remaining",
              remaining);
        }

        // now close it.
        // if there is still data in the stream, the SDK
        // will warn and escalate to an abort itself.
        LOG.debug("Closing stream");
        sdkStream.close();

        // this MUST come after the close, so that if the IO operations fail
        // and an abort is triggered, the initial attempt's statistics
        // aren't collected.
        streamStatistics.streamClose(false, drained);
        return false;
      } catch (Exception e) {
        // exception escalates to an abort
        LOG.debug("When closing {} stream for {}, will abort the stream",
            uri, reason, e);
        thrown = e;
      }
    }
    // Abort, rather than just close, the underlying stream. Otherwise, the
    // remaining object payload is read from S3 while closing the stream.
    LOG.debug("Aborting stream {}", uri);
    try {
      sdkStream.abort();
    } catch (Exception e) {
      LOG.warn("When aborting {} stream after failing to close it for {}",
          uri, reason, e);
      thrown = e;
    }

    streamStatistics.streamClose(true, remaining);
    LOG.debug("Stream {} aborted: {}; remaining={}",
        uri, reason, remaining);
    return true;
  }

  public String getUri() {
    return uri;
  }

  public TStream getSdkStream() {
    return sdkStream;
  }

  public boolean shouldAbort() {
    return shouldAbort;
  }

  public int getRemaining() {
    return remaining;
  }

  public S3AInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  public String getReason() {
    return reason;
  }

  public boolean executed() {
    return executed.get();
  }

  public Exception getThrown() {
    return thrown;
  }

  public int getDrained() {
    return drained;
  }

  public boolean aborted() {
    return aborted;
  }

  @Override
  public String toString() {
    return "SDKStreamDrainer{" +
        "uri='" + uri + '\'' +
        ", reason='" + reason + '\'' +
        ", shouldAbort=" + shouldAbort +
        ", remaining=" + remaining +
        ", executed=" + executed.get() +
        ", aborted=" + aborted +
        ", inner=" + sdkStream +
        ", thrown=" + thrown +
        '}';
  }
}
