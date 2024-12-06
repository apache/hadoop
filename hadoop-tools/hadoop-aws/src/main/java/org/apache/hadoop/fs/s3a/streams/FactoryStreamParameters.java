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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;

import static java.util.Objects.requireNonNull;

/**
 * Parameters for input streams created through
 * {@link InputStreamFactory}.
 * It is designed to be extensible; the {@link #build()}
 * operation does not freeze the parameters -instead it simply
 * verifies that all required values are set.
 */
public final class FactoryStreamParameters {

  private S3AReadOpContext context;

  private S3ObjectAttributes objectAttributes;

  private StreamReadCallbacks callbacks;

  private S3AInputStreamStatistics streamStatistics;

  private ExecutorService boundedThreadPool;

  /**
   * Read operation context.
   */
  public S3AReadOpContext getContext() {
    return context;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public FactoryStreamParameters withContext(S3AReadOpContext value) {
    context = value;
    return this;
  }

  /**
   * Attributes of the object.
   */
  public S3ObjectAttributes getObjectAttributes() {
    return objectAttributes;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public FactoryStreamParameters withObjectAttributes(S3ObjectAttributes value) {
    objectAttributes = value;
    return this;
  }

  /**
   * Callbacks to the store.
   */
  public StreamReadCallbacks getCallbacks() {
    return callbacks;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public FactoryStreamParameters withCallbacks(StreamReadCallbacks value) {
    callbacks = value;
    return this;
  }

  /**
   * Stream statistics.
   */
  public S3AInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public FactoryStreamParameters withStreamStatistics(S3AInputStreamStatistics value) {
    streamStatistics = value;
    return this;
  }

  /**
   * Bounded thread pool for submitting asynchronous
   * work.
   */
  public ExecutorService getBoundedThreadPool() {
    return boundedThreadPool;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public FactoryStreamParameters withBoundedThreadPool(ExecutorService value) {
    boundedThreadPool = value;
    return this;
  }

  /**
   * Validate that all attributes are as expected.
   * Mock tests can skip this if required.
   * @return the object.
   */
  public FactoryStreamParameters build() {
    requireNonNull(boundedThreadPool, "boundedThreadPool");
    requireNonNull(callbacks, "callbacks");
    requireNonNull(context, "context");
    requireNonNull(objectAttributes, "objectAttributes");
    requireNonNull(streamStatistics, "streamStatistics");
    requireNonNull(boundedThreadPool, "boundedThreadPool");
    return this;
  }
}
