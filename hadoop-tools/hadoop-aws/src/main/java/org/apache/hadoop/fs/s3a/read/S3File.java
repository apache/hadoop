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

package org.apache.hadoop.fs.s3a.read;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.fs.common.Io;
import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Ecapsulates low level interactions with S3 object on AWS.
 */
public class S3File implements Closeable {

  // Read-specific operation context.
  private final S3AReadOpContext context;

  // S3 object attributes.
  private final S3ObjectAttributes s3Attributes;

  // Callbacks used for interacting with the underlying S3 client.
  private final S3AInputStream.InputStreamCallbacks client;

  // Used for reporting input stream access statistics.
  private final S3AInputStreamStatistics streamStatistics;

  // Enforces change tracking related policies.
  private final ChangeTracker changeTracker;

  // Maps a stream returned by openForRead() to the associated S3 object.
  // That allows us to close the object when closing the stream.
  private Map<InputStream, S3Object> s3Objects;

  /**
   * Initializes a new instance of the {@code S3File} class.
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
  public S3File(
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
    this.s3Objects = new IdentityHashMap<InputStream, S3Object>();
  }

  /**
   * Gets an instance of {@code Invoker} for interacting with S3 API.
   *
   * @return an instance of {@code Invoker} for interacting with S3 API.
   */
  public Invoker getReadInvoker() {
    return this.context.getReadInvoker();
  }

  /**
   * Gets an instance of {@code S3AInputStreamStatistics} used for reporting access metrics.
   *
   * @return an instance of {@code S3AInputStreamStatistics} used for reporting access metrics.
   */
  public S3AInputStreamStatistics getStatistics() {
    return this.streamStatistics;
  }

  /**
   * Gets the path of this file.
   *
   * @return the path of this file.
   */
  public String getPath() {
    return getPath(this.s3Attributes);
  }

  /**
   * Gets the path corresponding to the given s3Attributes.
   *
   * @param s3Attributes attributes of an S3 object.
   * @return the path corresponding to the given s3Attributes.
   */
  public static String getPath(S3ObjectAttributes s3Attributes) {
    return String.format("s3a://%s/%s", s3Attributes.getBucket(), s3Attributes.getKey());
  }

  /**
   * Gets the size of this file.
   * Its value is cached once obtained from AWS.
   *
   * @return the size of this file.
   */
  public long size() {
    return this.s3Attributes.getLen();
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

    final GetObjectRequest request = client.newGetRequest(this.s3Attributes.getKey())
        .withRange(offset, offset + size - 1);
    this.changeTracker.maybeApplyConstraint(request);

    String uri = this.getPath();
    String operation = String.format(
        "%s %s at %d", S3AInputStream.OPERATION_OPEN, uri, offset);
    DurationTracker tracker = streamStatistics.initiateGetRequest();
    S3Object object = null;

    try {
      object = Invoker.once(operation, uri, () -> client.getObject(request));
    } catch(IOException e) {
      tracker.failed();
      throw e;
    } finally {
      tracker.close();
    }

    changeTracker.processResponse(object, operation, offset);
    InputStream stream = object.getObjectContent();
    synchronized (this.s3Objects) {
      this.s3Objects.put(stream, object);
    }

    return stream;
  }

  /**
   * Closes this stream and releases all acquired resources.
   */
  @Override
  public synchronized void close() {
    List<InputStream> streams = new ArrayList<InputStream>(this.s3Objects.keySet());
    for (InputStream stream : streams) {
      this.close(stream);
    }
  }

  void close(InputStream inputStream) {
    S3Object obj;
    synchronized (this.s3Objects) {
      obj = this.s3Objects.get(inputStream);
      if (obj == null) {
        throw new IllegalArgumentException("inputStream not found");
      }
      this.s3Objects.remove(inputStream);
    }

    Io.closeIgnoringIoException(inputStream);
    Io.closeIgnoringIoException(obj);
  }
}
