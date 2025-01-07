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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import static java.util.Objects.requireNonNull;

/**
 * Parameters for object input streams created through
 * {@link ObjectInputStreamFactory}.
 * It is designed to support extra parameters added
 * in future.
 * <p>Note that the {@link #validate()}
 * operation does not freeze the parameters -instead it simply
 * verifies that all required values are set.
 */
public final class ObjectReadParameters {

  /**
   *  Read operation context.
   */
  private S3AReadOpContext context;

  /**
   * Attributes of the object.
   */
  private S3ObjectAttributes objectAttributes;

  /**
   * Callbacks to the store.
   */
  private ObjectInputStreamCallbacks callbacks;

  /**
   * Stream statistics.
   */
  private S3AInputStreamStatistics streamStatistics;

  /**
   * Bounded thread pool for submitting asynchronous
   * work.
   */
  private ExecutorService boundedThreadPool;

  /**
   * Allocator of local FS storage.
   */
  private LocalDirAllocator directoryAllocator;

  /**
   * @return Read operation context.
   */
  public S3AReadOpContext getContext() {
    return context;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withContext(S3AReadOpContext value) {
    context = value;
    return this;
  }

  /**
   * @return Attributes of the object.
   */
  public S3ObjectAttributes getObjectAttributes() {
    return objectAttributes;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withObjectAttributes(S3ObjectAttributes value) {
    objectAttributes = value;
    return this;
  }

  /**
   * @return callbacks to the store.
   */
  public ObjectInputStreamCallbacks getCallbacks() {
    return callbacks;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withCallbacks(ObjectInputStreamCallbacks value) {
    callbacks = value;
    return this;
  }

  /**
   * @return Stream statistics.
   */
  public S3AInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withStreamStatistics(S3AInputStreamStatistics value) {
    streamStatistics = value;
    return this;
  }

  /**
   * @return Bounded thread pool for submitting asynchronous work.
   */
  public ExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withBoundedThreadPool(ExecutorService value) {
    boundedThreadPool = value;
    return this;
  }

  public LocalDirAllocator getDirectoryAllocator() {
    return directoryAllocator;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public ObjectReadParameters withDirectoryAllocator(final LocalDirAllocator value) {
    directoryAllocator = value;
    return this;
  }

  /**
   * Validate that all attributes are as expected.
   * Mock tests can skip this if required.
   * @return the object.
   */
  public ObjectReadParameters validate() {
    // please keep in alphabetical order.
    requireNonNull(boundedThreadPool, "boundedThreadPool");
    requireNonNull(callbacks, "callbacks");
    requireNonNull(context, "context");
    requireNonNull(directoryAllocator, "directoryAllocator");
    requireNonNull(objectAttributes, "objectAttributes");
    requireNonNull(streamStatistics, "streamStatistics");
    return this;
  }
}
