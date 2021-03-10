/**
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

package org.apache.hadoop.fs.azurebfs.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold extra input stream configs.
 */
public class AbfsInputStreamContext extends AbfsStreamContext {
  // Retaining logger of AbfsInputStream
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);

  private int readBufferSize;

  private int readAheadQueueDepth;

  private boolean tolerateOobAppends;

  private boolean alwaysReadBufferSize;

  private int readAheadBlockSize;

  private AbfsInputStreamStatistics streamStatistics;

  private boolean readSmallFilesCompletely;

  private boolean optimizeFooterRead;

  private boolean bufferedPreadDisabled;

  public AbfsInputStreamContext(final long sasTokenRenewPeriodForStreamsInSeconds) {
    super(sasTokenRenewPeriodForStreamsInSeconds);
  }

  public AbfsInputStreamContext withReadBufferSize(final int readBufferSize) {
    this.readBufferSize = readBufferSize;
    return this;
  }

  public AbfsInputStreamContext withReadAheadQueueDepth(
          final int readAheadQueueDepth) {
    this.readAheadQueueDepth = (readAheadQueueDepth >= 0)
            ? readAheadQueueDepth
            : Runtime.getRuntime().availableProcessors();
    return this;
  }

  public AbfsInputStreamContext withTolerateOobAppends(
          final boolean tolerateOobAppends) {
    this.tolerateOobAppends = tolerateOobAppends;
    return this;
  }

  public AbfsInputStreamContext withStreamStatistics(
      final AbfsInputStreamStatistics streamStatistics) {
    this.streamStatistics = streamStatistics;
    return this;
  }

  public AbfsInputStreamContext withReadSmallFilesCompletely(
      final boolean readSmallFilesCompletely) {
    this.readSmallFilesCompletely = readSmallFilesCompletely;
    return this;
  }

  public AbfsInputStreamContext withOptimizeFooterRead(
      final boolean optimizeFooterRead) {
    this.optimizeFooterRead = optimizeFooterRead;
    return this;
  }

  public AbfsInputStreamContext withShouldReadBufferSizeAlways(
      final boolean alwaysReadBufferSize) {
    this.alwaysReadBufferSize = alwaysReadBufferSize;
    return this;
  }

  public AbfsInputStreamContext withReadAheadBlockSize(
      final int readAheadBlockSize) {
    this.readAheadBlockSize = readAheadBlockSize;
    return this;
  }

  public AbfsInputStreamContext withBufferedPreadDisabled(
      final boolean bufferedPreadDisabled) {
    this.bufferedPreadDisabled = bufferedPreadDisabled;
    return this;
  }

  public AbfsInputStreamContext build() {
    if (readBufferSize > readAheadBlockSize) {
      LOG.debug(
          "fs.azure.read.request.size[={}] is configured for higher size than "
              + "fs.azure.read.readahead.blocksize[={}]. Auto-align "
              + "readAhead block size to be same as readRequestSize.",
          readBufferSize, readAheadBlockSize);
      readAheadBlockSize = readBufferSize;
    }
    // Validation of parameters to be done here.
    return this;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  public boolean isTolerateOobAppends() {
    return tolerateOobAppends;
  }

  public AbfsInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  public boolean readSmallFilesCompletely() {
    return this.readSmallFilesCompletely;
  }

  public boolean optimizeFooterRead() {
    return this.optimizeFooterRead;
  }

  public boolean shouldReadBufferSizeAlways() {
    return alwaysReadBufferSize;
  }

  public int getReadAheadBlockSize() {
    return readAheadBlockSize;
  }

  public boolean isBufferedPreadDisabled() {
    return bufferedPreadDisabled;
  }
}
