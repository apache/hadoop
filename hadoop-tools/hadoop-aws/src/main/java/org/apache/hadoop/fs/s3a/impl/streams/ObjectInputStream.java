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

package org.apache.hadoop.fs.s3a.impl.streams;

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
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * A stream of data from an S3 object.
 * <p>
 * The base class includes common methods, stores
 * common data and incorporates leak tracking.
 */
public abstract class ObjectInputStream extends FSInputStream
    implements StreamCapabilities, IOStatisticsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectInputStream.class);

  /**
   * IOStatistics report.
   */
  private final IOStatistics ioStatistics;

  /**
   * Read-specific operation context structure.
   */
  private final S3AReadOpContext context;

  /**
   * Callbacks for reading input stream data from the S3 Store.
   */
  private final ObjectInputStreamCallbacks callbacks;

  /**
   * Thread pool used for bounded IO operations.
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

  /**
   * Attributes of the remote object.
   */
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
   * Stream type.
   */
  private final InputStreamType streamType;

  /**
   * Requested input policy.
   */
  private S3AInputPolicy inputPolicy;


  /** Vectored IO context. */
  private final VectoredIOContext vectoredIOContext;

  /**
   * Constructor.
   * @param streamType stream type enum.
   * @param parameters extensible parameter list.
   */
  protected ObjectInputStream(
      final InputStreamType streamType,
      final ObjectReadParameters parameters) {

    this.streamType = requireNonNull(streamType);
    this.objectAttributes = parameters.getObjectAttributes();
    checkArgument(isNotEmpty(objectAttributes.getBucket()),
        "No Bucket");
    checkArgument(isNotEmpty(objectAttributes.getKey()), "No Key");
    long l = objectAttributes.getLen();
    checkArgument(l >= 0, "Negative content length");
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
        this::abortInFinalizer);
    this.vectoredIOContext = getContext().getVectoredIOContext();
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

  /**
   * Declare the base capabilities implemented by this class and so by
   * all subclasses.
   * <p>
   * Subclasses MUST override this if they add more capabilities,
   * or actually remove any of these.
   * @param capability string to query the stream support for.
   * @return true if all implementations are known to have the specific
   * capability.
   */
  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamStatisticNames.STREAM_LEAKS:
      return true;
    default:
      // dynamic probe for the name of this stream
      if (streamType.capability().equals(capability)) {
        return true;
      }
      return false;
    }
  }

  /**
   * Read-specific operation context structure.
   * @return Read-specific operation context structure.
   */
  protected final S3AReadOpContext getContext() {
    return context;
  }

  /**
   * Callbacks for reading input stream data from the S3 Store.
   * @return Callbacks for reading input stream data from the S3 Store.
   */
  protected final ObjectInputStreamCallbacks getCallbacks() {
    return callbacks;
  }

  /**
   * Thread pool used for bounded IO operations.
   * @return Thread pool used for bounded IO operations.
   */
  protected final ExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  /**
   * URI of path.
   * @return URI of path.
   */
  protected final String getUri() {
    return uri;
  }

  /**
   * Store bucket.
   * @return Store bucket.
   */
  protected final String getBucket() {
    return bucket;
  }

  /**
   * Store key.
   * @return Store key.
   */
  protected final String getKey() {
    return key;
  }

  /**
   * Path URI as a string.
   * @return Path URI as a string.
   */
  protected final String getPathStr() {
    return pathStr;
  }

  /**
   * Content length from HEAD or openFile option.
   * @return Content length from HEAD or openFile option.
   */
  protected final long getContentLength() {
    return contentLength;
  }

  /**
   * Aggregator used to aggregate per thread IOStatistics.
   * @return Aggregator used to aggregate per thread IOStatistics.
   */
  protected final IOStatisticsAggregator getThreadIOStatistics() {
    return threadIOStatistics;
  }

  /**
   * Attributes of the remote object.
   * @return Attributes of the remote object.
   */
  protected final S3ObjectAttributes getObjectAttributes() {
    return objectAttributes;
  }

  /**
   * Get Vectored IO context.
   * @return Vectored IO context.
   */
  protected VectoredIOContext getVectoredIOContext() {
    return vectoredIOContext;
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int minSeekForVectorReads() {
    return vectoredIOContext.getMinSeekForVectorReads();
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int maxReadSizeForVectorReads() {
    return vectoredIOContext.getMaxReadSizeForVectorReads();
  }

  public InputStreamType streamType() {
    return streamType;
  }

  @Override
  public String toString() {
    return "ObjectInputStream{" +
        "streamType=" + streamType +
        ", uri='" + uri + '\'' +
        ", contentLength=" + contentLength +
        ", inputPolicy=" + inputPolicy +
        ", vectoredIOContext=" + vectoredIOContext +
        "} " + super.toString();
  }
}
