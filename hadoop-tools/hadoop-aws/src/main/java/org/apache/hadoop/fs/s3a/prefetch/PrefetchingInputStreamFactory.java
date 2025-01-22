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

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.streams.AbstractObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.StreamThreadOptions;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_COUNT_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_COUNT;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.S3AUtils.intOption;
import static org.apache.hadoop.fs.s3a.S3AUtils.longBytesOption;
import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Factory for prefetching streams.
 * <p>
 * Reads and validates prefetch configuration options during service init.
 */
public class PrefetchingInputStreamFactory extends AbstractObjectInputStreamFactory {

  /** Size in bytes of a single prefetch block. */
  private int prefetchBlockSize;

  /** Size of prefetch queue (in number of blocks). */
  private int prefetchBlockCount;

  /**
   * Shared prefetch options.
   */
  private PrefetchOptions prefetchOptions;

  public PrefetchingInputStreamFactory() {
    super("PrefetchingInputStreamFactory");
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    long prefetchBlockSizeLong =
        longBytesOption(conf, PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE, 1);
    checkState(prefetchBlockSizeLong < Integer.MAX_VALUE,
        "S3A prefetch block size exceeds int limit");
    prefetchBlockSize = (int) prefetchBlockSizeLong;
    prefetchBlockCount =
        intOption(conf, PREFETCH_BLOCK_COUNT_KEY, PREFETCH_BLOCK_DEFAULT_COUNT, 1);

    prefetchOptions = new PrefetchOptions(
        prefetchBlockSize,
        prefetchBlockCount);
  }

  @Override
  public ObjectInputStream readObject(final ObjectReadParameters parameters) throws IOException {
    return new S3APrefetchingInputStream(parameters,
        getConfig(),
        prefetchOptions);
  }

  /**
   * The thread count is calculated from the configuration.
   * @return a positive thread count.
   */
  @Override
  public StreamThreadOptions threadRequirements() {
    return new StreamThreadOptions(prefetchBlockCount, 0, true, false);
  }

}
