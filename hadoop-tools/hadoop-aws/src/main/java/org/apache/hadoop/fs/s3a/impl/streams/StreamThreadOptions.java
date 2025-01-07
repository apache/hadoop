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

/**
 * Options for threading on this input stream.
 */
public class StreamThreadOptions {

  /** Number of shared threads to included in the bounded pool. */
  private final int sharedThreads;

  /**
   * How many threads per stream, ignoring vector IO requirements.
   */
  private final int streamThreads;

  /**
   * Flag to enable creation of a future pool around the bounded thread pool.
   */
  private final boolean createFuturePool;

  /**
   * Is vector IO supported (so its thread requirements
   * included too)?
   */
  private final boolean vectorSupported;

  /**
   * Create the thread options.
   * @param sharedThreads Number of shared threads to included in the bounded pool.
   * @param streamThreads How many threads per stream, ignoring vector IO requirements.
   * @param createFuturePool Flag to enable creation of a future pool around the bounded thread pool.
   */
  public StreamThreadOptions(final int sharedThreads,
      final int streamThreads,
      final boolean createFuturePool,
      final boolean vectorSupported) {
    this.sharedThreads = sharedThreads;
    this.streamThreads = streamThreads;
    this.createFuturePool = createFuturePool;
    this.vectorSupported = vectorSupported;
  }

  public int sharedThreads() {
    return sharedThreads;
  }

  public int streamThreads() {
    return streamThreads;
  }

  public boolean createFuturePool() {
    return createFuturePool;
  }

  public boolean vectorSupported() {
    return vectorSupported;
  }
}
