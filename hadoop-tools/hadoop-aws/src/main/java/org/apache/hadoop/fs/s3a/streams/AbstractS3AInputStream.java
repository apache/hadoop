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

package org.apache.hadoop.fs.s3a.streams;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.LeakReporter;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.util.Preconditions;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Base class for input streams returned by the factory, and therefore
 * used within S3A code.
 */
public abstract class AbstractS3AInputStream extends FSInputStream
    implements StreamCapabilities, IOStatisticsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3AInputStream.class);

  /**
   * IOStatistics report.
   */
  protected final IOStatistics ioStatistics;

  /**
   * Read-specific operation context structure.
   */
  private final S3AReadOpContext context;

  /**
   * Callbacks for reading input stream data from the S3 Store.
   */
  private final StreamReadCallbacks callbacks;

  /**
   * Thread pool used for vectored IO operation.
   */
  private final ExecutorService boundedThreadPool;

  /**
   * URI of path.
   */
  private final String uri;

  /**
   * Store bucket.
   */
  private final String bucket;

  /**
   * Store key.
   */
  private final String key;

  /**
   * Path URI as a string.
   */
  private final String pathStr;

  /**
   * Content length from HEAD or openFile option.
   */
  private final long contentLength;

  private final S3ObjectAttributes objectAttributes;

  /**
   * Stream statistics.
   */
  private final S3AInputStreamStatistics streamStatistics;

  /** Aggregator used to aggregate per thread IOStatistics. */
  private final IOStatisticsAggregator threadIOStatistics;

  /**
   * Report of leaks.
   * with report and abort unclosed streams in finalize().
   */
  private final LeakReporter leakReporter;

  /**
   * Requested input policy.
   */
  private S3AInputPolicy inputPolicy;

  /**
   * Constructor.
   * @param parameters extensible parameter list.
   */
  protected AbstractS3AInputStream(
      FactoryStreamParameters parameters) {

    objectAttributes = parameters.getObjectAttributes();
    Preconditions.checkArgument(isNotEmpty(objectAttributes.getBucket()),
        "No Bucket");
    Preconditions.checkArgument(isNotEmpty(objectAttributes.getKey()), "No Key");
    long l = objectAttributes.getLen();
    Preconditions.checkArgument(l >= 0, "Negative content length");
    this.context = parameters.getContext();
    this.contentLength = l;

    this.bucket = objectAttributes.getBucket();
    this.key = objectAttributes.getKey();
    this.pathStr = objectAttributes.getPath().toString();
    this.callbacks = parameters.getCallbacks();
    this.uri = "s3a://" + bucket + "/" + key;
    this.streamStatistics = parameters.getStreamStatistics();
    this.ioStatistics = streamStatistics.getIOStatistics();
    this.inputPolicy = context.getInputPolicy();
    streamStatistics.inputPolicySet(inputPolicy.ordinal());
    this.boundedThreadPool = parameters.getBoundedThreadPool();
    this.threadIOStatistics = requireNonNull(context.getIOStatisticsAggregator());
    // build the leak reporter
    this.leakReporter = new LeakReporter(
        "Stream not closed while reading " + uri,
        this::isStreamOpen,
        () -> abortInFinalizer());
  }

  /**
   * Probe for stream being open.
   * Not synchronized; the flag is volatile.
   * @return true if the stream is still open.
   */
  protected abstract boolean isStreamOpen();

  /**
   * Brute force stream close; invoked by {@link LeakReporter}.
   * All exceptions raised are ignored.
   */
  protected abstract void abortInFinalizer();

  /**
   * Close the stream.
   * This triggers publishing of the stream statistics back to the filesystem
   * statistics.
   * This operation is synchronized, so that only one thread can attempt to
   * @throws IOException on any problem
   */
  @Override
  public synchronized void close() throws IOException {
    // end the client+audit span.
    callbacks.close();
    // merge the statistics back into the FS statistics.
    streamStatistics.close();
    // Collect ThreadLevel IOStats
    mergeThreadIOStatistics(streamStatistics.getIOStatistics());
  }

  /**
   * Merging the current thread's IOStatistics with the current IOStatistics
   * context.
   * @param streamIOStats Stream statistics to be merged into thread
   * statistics aggregator.
   */
  protected void mergeThreadIOStatistics(IOStatistics streamIOStats) {
    threadIOStatistics.aggregate(streamIOStats);
  }

  /**
   * Finalizer.
   * <p>
   * Verify that the inner stream is closed.
   * <p>
   * If it is not, it means streams are being leaked in application code.
   * Log a warning, including the stack trace of the caller,
   * then abort the stream.
   * <p>
   * This does not attempt to invoke {@link #close()} as that is
   * a more complex operation, and this method is being executed
   * during a GC finalization phase.
   * <p>
   * Applications MUST close their streams; this is a defensive
   * operation to return http connections and warn the end users
   * that their applications are at risk of running out of connections.
   *
   * {@inheritDoc}
   */
  @Override
  protected void finalize() throws Throwable {
    leakReporter.close();
    super.finalize();
  }

  /**
   * Get the current input policy.
   * @return input policy.
   */
  @VisibleForTesting
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Set/update the input policy of the stream.
   * This updates the stream statistics.
   * @param inputPolicy new input policy.
   */
  protected void setInputPolicy(S3AInputPolicy inputPolicy) {
    LOG.debug("Switching to input policy {}", inputPolicy);
    this.inputPolicy = inputPolicy;
    streamStatistics.inputPolicySet(inputPolicy.ordinal());
  }

  /**
   * Access the input stream statistics.
   * This is for internal testing and may be removed without warning.
   * @return the statistics for this input stream
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamCapabilities.IOSTATISTICS_CONTEXT:
    case StreamStatisticNames.STREAM_LEAKS:
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.UNBUFFER:
    case StreamCapabilities.VECTOREDIO:
      return true;
    default:
      return false;
    }
  }


  protected S3AReadOpContext getContext() {
    return context;
  }

  protected StreamReadCallbacks getCallbacks() {
    return callbacks;
  }

  protected ExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  protected String getUri() {
    return uri;
  }

  protected String getBucket() {
    return bucket;
  }

  protected String getKey() {
    return key;
  }

  protected String getPathStr() {
    return pathStr;
  }

  protected long getContentLength() {
    return contentLength;
  }

  protected S3AInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  protected IOStatisticsAggregator getThreadIOStatistics() {
    return threadIOStatistics;
  }

  protected S3ObjectAttributes getObjectAttributes() {
    return objectAttributes;
  }
}


