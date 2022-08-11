/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.prefetch;


import java.io.IOException;
import java.io.InputStream;
import java.util.IdentityHashMap;
import java.util.Map;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Encapsulates low level interactions with S3 object on AWS.
 */
public class S3ARemoteObject {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ARemoteObject.class);

  /**
   * Read-specific operation context.
   */
  private final S3AReadOpContext context;

  /**
   * S3 object attributes.
   */
  private final S3ObjectAttributes s3Attributes;

  /**
   * Callbacks used for interacting with the underlying S3 client.
   */
  private final S3AInputStream.InputStreamCallbacks client;

  /**
   * Used for reporting input stream access statistics.
   */
  private final S3AInputStreamStatistics streamStatistics;

  /**
   * Enforces change tracking related policies.
   */
  private final ChangeTracker changeTracker;

  /**
   * Maps a stream returned by openForRead() to the associated S3 object.
   * That allows us to close the object when closing the stream.
   */
  private Map<InputStream, S3Object> s3Objects;

  /**
   * uri of the object being read.
   */
  private final String uri;

  /**
   * size of a buffer to create when draining the stream.
   */
  private static final int DRAIN_BUFFER_SIZE = 16384;

  /**
   * Initializes a new instance of the {@code S3ARemoteObject} class.
   *
   * @param context read-specific operation context.
   * @param s3Attributes attributes of the S3 object being read.
   * @param client callbacks used for interacting with the underlying S3 client.
   * @param streamStatistics statistics about stream access.
   * @param changeTracker helps enforce change tracking policy.
   *
   * @throws IllegalArgumentException if context is null.
   * @throws IllegalArgumentException if s3Attributes is null.
   * @throws IllegalArgumentException if client is null.
   * @throws IllegalArgumentException if streamStatistics is null.
   * @throws IllegalArgumentException if changeTracker is null.
   */
  public S3ARemoteObject(
      S3AReadOpContext context,
      S3ObjectAttributes s3Attributes,
      S3AInputStream.InputStreamCallbacks client,
      S3AInputStreamStatistics streamStatistics,
      ChangeTracker changeTracker) {

    Validate.checkNotNull(context, "context");
    Validate.checkNotNull(s3Attributes, "s3Attributes");
    Validate.checkNotNull(client, "client");
    Validate.checkNotNull(streamStatistics, "streamStatistics");
    Validate.checkNotNull(changeTracker, "changeTracker");

    this.context = context;
    this.s3Attributes = s3Attributes;
    this.client = client;
    this.streamStatistics = streamStatistics;
    this.changeTracker = changeTracker;
    this.s3Objects = new IdentityHashMap<>();
    this.uri = this.getPath();
  }

  /**
   * Gets an instance of {@code Invoker} for interacting with S3 API.
   *
   * @return an instance of {@code Invoker} for interacting with S3 API.
   */
  public Invoker getReadInvoker() {
    return context.getReadInvoker();
  }

  /**
   * Gets an instance of {@code S3AInputStreamStatistics} used for reporting access metrics.
   *
   * @return an instance of {@code S3AInputStreamStatistics} used for reporting access metrics.
   */
  public S3AInputStreamStatistics getStatistics() {
    return streamStatistics;
  }

  /**
   * Gets the path of this file.
   *
   * @return the path of this file.
   */
  public String getPath() {
    return getPath(s3Attributes);
  }

  /**
   * Gets the path corresponding to the given s3Attributes.
   *
   * @param s3Attributes attributes of an S3 object.
   * @return the path corresponding to the given s3Attributes.
   */
  public static String getPath(S3ObjectAttributes s3Attributes) {
    return String.format("s3a://%s/%s", s3Attributes.getBucket(),
        s3Attributes.getKey());
  }

  /**
   * Gets the size of this file.
   * Its value is cached once obtained from AWS.
   *
   * @return the size of this file.
   */
  public long size() {
    return s3Attributes.getLen();
  }

  /**
   * Opens a section of the file for reading.
   *
   * @param offset Start offset (0 based) of the section to read.
   * @param size Size of the section to read.
   * @return an {@code InputStream} corresponding to the given section of this file.
   *
   * @throws IOException if there is an error opening this file section for reading.
   * @throws IllegalArgumentException if offset is negative.
   * @throws IllegalArgumentException if offset is greater than or equal to file size.
   * @throws IllegalArgumentException if size is greater than the remaining bytes.
   */
  public InputStream openForRead(long offset, int size) throws IOException {
    Validate.checkNotNegative(offset, "offset");
    Validate.checkLessOrEqual(offset, "offset", size(), "size()");
    Validate.checkLessOrEqual(size, "size", size() - offset, "size() - offset");

    streamStatistics.streamOpened();
    final GetObjectRequest request =
        client.newGetRequest(s3Attributes.getKey())
            .withRange(offset, offset + size - 1);
    changeTracker.maybeApplyConstraint(request);

    String operation = String.format(
        "%s %s at %d", S3AInputStream.OPERATION_OPEN, uri, offset);
    DurationTracker tracker = streamStatistics.initiateGetRequest();
    S3Object object = null;

    try {
      object = Invoker.once(operation, uri, () -> client.getObject(request));
    } catch (IOException e) {
      tracker.failed();
      throw e;
    } finally {
      tracker.close();
    }

    changeTracker.processResponse(object, operation, offset);
    InputStream stream = object.getObjectContent();
    synchronized (s3Objects) {
      s3Objects.put(stream, object);
    }

    return stream;
  }

  void close(InputStream inputStream, int numRemainingBytes) {
    S3Object obj;
    synchronized (s3Objects) {
      obj = s3Objects.get(inputStream);
      if (obj == null) {
        throw new IllegalArgumentException("inputStream not found");
      }
      s3Objects.remove(inputStream);
    }

    if (numRemainingBytes <= context.getAsyncDrainThreshold()) {
      // don't bother with async io.
      drain(false, "close() operation", numRemainingBytes, obj, inputStream);
    } else {
      LOG.debug("initiating asynchronous drain of {} bytes", numRemainingBytes);
      // schedule an async drain/abort with references to the fields so they
      // can be reused
      client.submit(
          () -> drain(false, "close() operation", numRemainingBytes, obj,
              inputStream));
    }
  }

  /**
   * drain the stream. This method is intended to be
   * used directly or asynchronously, and measures the
   * duration of the operation in the stream statistics.
   *
   * @param shouldAbort   force an abort; used if explicitly requested.
   * @param reason        reason for stream being closed; used in messages
   * @param remaining     remaining bytes
   * @param requestObject http request object;
   * @param inputStream   stream to close.
   * @return was the stream aborted?
   */
  private boolean drain(
      final boolean shouldAbort,
      final String reason,
      final long remaining,
      final S3Object requestObject,
      final InputStream inputStream) {

    try {
      return invokeTrackingDuration(
          streamStatistics.initiateInnerStreamClose(shouldAbort),
          () -> drainOrAbortHttpStream(shouldAbort, reason, remaining,
              requestObject, inputStream));
    } catch (IOException e) {
      // this is only here because invokeTrackingDuration() has it in its
      // signature
      return shouldAbort;
    }
  }

  /**
   * Drain or abort the inner stream.
   * Exceptions are swallowed.
   * If a close() is attempted and fails, the operation escalates to
   * an abort.
   *
   * @param shouldAbort   force an abort; used if explicitly requested.
   * @param reason        reason for stream being closed; used in messages
   * @param remaining     remaining bytes
   * @param requestObject http request object
   * @param inputStream   stream to close.
   * @return was the stream aborted?
   */
  private boolean drainOrAbortHttpStream(
      boolean shouldAbort,
      final String reason,
      final long remaining,
      final S3Object requestObject,
      final InputStream inputStream) {

    if (!shouldAbort && remaining > 0) {
      try {
        long drained = 0;
        byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
        while (true) {
          final int count = inputStream.read(buffer);
          if (count < 0) {
            // no more data is left
            break;
          }
          drained += count;
        }
        LOG.debug("Drained stream of {} bytes", drained);
      } catch (Exception e) {
        // exception escalates to an abort
        LOG.debug("When closing {} stream for {}, will abort the stream", uri,
            reason, e);
        shouldAbort = true;
      }
    }
    cleanupWithLogger(LOG, inputStream);
    cleanupWithLogger(LOG, requestObject);
    streamStatistics.streamClose(shouldAbort, remaining);

    LOG.debug("Stream {} {}: {}; remaining={}", uri,
        (shouldAbort ? "aborted" : "closed"), reason,
        remaining);
    return shouldAbort;
  }
}
