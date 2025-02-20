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

import java.util.Arrays;
import java.util.EnumSet;

import org.apache.hadoop.fs.s3a.VectoredIOContext;

/**
 * Requirements for requirements for streams from this factory,
 * including threading and vector IO, and of
 * the Filesystem instance itself via
 * {@link Requirements}.
 * The FS is expected to adapt its internal configuration based on
 * the requirements passed back by the stream factory after its
 * creation.
 */
public class StreamFactoryRequirements {

  /**
   * Number of shared threads to included in the bounded pool.
   */
  private final int sharedThreads;

  /**
   * How many threads per stream, ignoring vector IO requirements?
   */
  private final int streamThreads;

  /**
   * VectoredIO behaviour.
   */
  private final VectoredIOContext vectoredIOContext;

  /**
   * Set of requirement flags.
   */
  private final EnumSet<Requirements> requirementFlags;

  /**
   * Create the thread options.
   * @param sharedThreads Number of shared threads to included in the bounded pool.
   * @param streamThreads How many threads per stream, ignoring vector IO requirements.
   * @param vectoredIOContext vector IO settings -made immutable if not already done.
   * @param requirements requirement flags of the factory and stream.
   */
  public StreamFactoryRequirements(
      final int sharedThreads,
      final int streamThreads,
      final VectoredIOContext vectoredIOContext,
      final Requirements...requirements) {
    this.sharedThreads = sharedThreads;
    this.streamThreads = streamThreads;
    this.vectoredIOContext = vectoredIOContext.build();
    if (requirements.length == 0) {
      this.requirementFlags = EnumSet.noneOf(Requirements.class);
    } else {
      this.requirementFlags = EnumSet.copyOf((Arrays.asList(requirements)));
    }
  }

  /**
   * Number of shared threads to included in the bounded pool.
   * @return extra threads to be created in the FS thread pool.
   */
  public int sharedThreads() {
    return sharedThreads;
  }

  /**
   * The maximum number of threads which can be used should by a single input stream.
   * @return thread pool requirements.
   */
  public int streamThreads() {
    return streamThreads;
  }

  /**
   * Should the future pool be created?
   * @return true if the future pool is required.
   */
  public boolean requiresFuturePool() {
    return requires(Requirements.RequiresFuturePool);
  }

  /**
   * The VectorIO requirements of streams.
   * @return vector IO context.
   */
  public VectoredIOContext vectoredIOContext() {
    return vectoredIOContext;
  }

  /**
   * Does this factory have this requirement?
   * @param r requirement to probe for.
   * @return true if this is a requirement.
   */
  public boolean requires(Requirements r) {
    return requirementFlags.contains(r);
  }

  @Override
  public String toString() {
    return "StreamFactoryRequirements{" +
        "sharedThreads=" + sharedThreads +
        ", streamThreads=" + streamThreads +
        ", requirementFlags=" + requirementFlags +
        ", vectoredIOContext=" + vectoredIOContext +
        '}';
  }

  /**
   * Requirements a factory may have.
   */
  public enum Requirements {

    /**
     * Expect Unaudited GETs.
     * Disables auditor warning/errors about GET requests being
     * issued outside an audit span.
     */
    ExpectUnauditedGetRequests,

    /**
     * Requires a future pool bound to the thread pool.
     */
    RequiresFuturePool

  }
}
