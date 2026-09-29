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

import org.apache.hadoop.fs.impl.BackReference;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;

/**
 * Class to hold extra input stream configs.
 */
public class AbfsInputStreamContext extends AbfsStreamContext {
  // Retaining logger of AbfsInputStream
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);

  private int readBufferSize;

  private int readAheadQueueDepth;

  private boolean tolerateOobAppends;

  private boolean isReadAheadEnabled = true;

  private boolean isReadAheadV2Enabled;

  private boolean alwaysReadBufferSize;

  private int readAheadBlockSize;

  private int readAheadRange;

  private AbfsInputStreamStatistics streamStatistics;

  private boolean readSmallFilesCompletely;

  private boolean optimizeFooterRead;

  private int footerReadBufferSize;

  private boolean bufferedPreadDisabled;

  private boolean restrictGpsOnOpenFile;

  /** A BackReference to the FS instance that created this OutputStream. */
  private BackReference fsBackRef;

  private ContextEncryptionAdapter contextEncryptionAdapter = null;

  /**
   * Constructs a new {@link AbfsInputStreamContext}.
   *
   * @param sasTokenRenewPeriodForStreamsInSeconds SAS token renewal interval in seconds.
   */
  public AbfsInputStreamContext(final long sasTokenRenewPeriodForStreamsInSeconds) {
    super(sasTokenRenewPeriodForStreamsInSeconds);
  }

  /**
   * Sets the read buffer size.
   *
   * @param readBufferSize buffer size in bytes.
   * @return this instance.
   */
  public AbfsInputStreamContext withReadBufferSize(final int readBufferSize) {
    this.readBufferSize = readBufferSize;
    return this;
  }

  /**
   * Sets the read-ahead queue depth.
   * Defaults to the number of available processors if negative.
   *
   * @param readAheadQueueDepth queue depth.
   * @return this instance.
   */
  public AbfsInputStreamContext withReadAheadQueueDepth(
          final int readAheadQueueDepth) {
    this.readAheadQueueDepth = (readAheadQueueDepth >= 0)
            ? readAheadQueueDepth
            : Runtime.getRuntime().availableProcessors();
    return this;
  }

  /**
   * Enables or disables tolerance for out-of-band appends.
   *
   * @param tolerateOobAppends whether OOB appends should be tolerated.
   * @return this instance.
   */
  public AbfsInputStreamContext withTolerateOobAppends(
          final boolean tolerateOobAppends) {
    this.tolerateOobAppends = tolerateOobAppends;
    return this;
  }

  /**
   * Enables or disables read-ahead feature.
   *
   * @param isReadAheadEnabled whether read-ahead is enabled.
   * @return this instance.
   */
  public AbfsInputStreamContext isReadAheadEnabled(
          final boolean isReadAheadEnabled) {
    this.isReadAheadEnabled = isReadAheadEnabled;
    return this;
  }

  /**
   * Enables or disables read-ahead version 2.
   *
   * @param isReadAheadV2Enabled whether read-ahead V2 is enabled.
   * @return this instance.
   */
  public AbfsInputStreamContext isReadAheadV2Enabled(
      final boolean isReadAheadV2Enabled) {
    this.isReadAheadV2Enabled = isReadAheadV2Enabled;
    return this;
  }

  /**
   * Sets the read-ahead range.
   *
   * @param readAheadRange range in bytes.
   * @return this instance.
   */
  public AbfsInputStreamContext withReadAheadRange(
          final int readAheadRange) {
    this.readAheadRange = readAheadRange;
    return this;
  }

  /**
   * Sets stream statistics collector.
   *
   * @param streamStatistics statistics instance.
   * @return this instance.
   */
  public AbfsInputStreamContext withStreamStatistics(
      final AbfsInputStreamStatistics streamStatistics) {
    this.streamStatistics = streamStatistics;
    return this;
  }

  /**
   * Enables or disables complete read of small files in a single operation.
   *
   * @param readSmallFilesCompletely whether small files should be fully read.
   * @return this instance.
   */
  public AbfsInputStreamContext withReadSmallFilesCompletely(
      final boolean readSmallFilesCompletely) {
    this.readSmallFilesCompletely = readSmallFilesCompletely;
    return this;
  }

  /**
   * Enables or disables footer read optimization.
   *
   * @param optimizeFooterRead whether footer read optimization is enabled.
   * @return this instance.
   */
  public AbfsInputStreamContext withOptimizeFooterRead(
      final boolean optimizeFooterRead) {
    this.optimizeFooterRead = optimizeFooterRead;
    return this;
  }

  /**
   * Sets the footer read buffer size.
   *
   * @param footerReadBufferSize size in bytes.
   * @return this instance.
   */
  public AbfsInputStreamContext withFooterReadBufferSize(final int footerReadBufferSize) {
    this.footerReadBufferSize = footerReadBufferSize;
    return this;
  }

  /**
   * Forces use of the configured read buffer size always.
   *
   * @param alwaysReadBufferSize whether to always use configured buffer size.
   * @return this instance.
   */
  public AbfsInputStreamContext withShouldReadBufferSizeAlways(
      final boolean alwaysReadBufferSize) {
    this.alwaysReadBufferSize = alwaysReadBufferSize;
    return this;
  }

  /**
   * Sets the read-ahead block size.
   *
   * @param readAheadBlockSize block size in bytes.
   * @return this instance.
   */
  public AbfsInputStreamContext withReadAheadBlockSize(
      final int readAheadBlockSize) {
    this.readAheadBlockSize = readAheadBlockSize;
    return this;
  }

  /**
   * Enables or disables buffered positional reads.
   *
   * @param bufferedPreadDisabled whether buffered pread is disabled.
   * @return this instance.
   */
  public AbfsInputStreamContext withBufferedPreadDisabled(
      final boolean bufferedPreadDisabled) {
    this.bufferedPreadDisabled = bufferedPreadDisabled;
    return this;
  }

  /**
   * Sets a back reference to the filesystem that created this stream.
   *
   * @param fsBackRef filesystem back reference.
   * @return this instance.
   */
  public AbfsInputStreamContext withAbfsBackRef(
      final BackReference fsBackRef) {
    this.fsBackRef = fsBackRef;
    return this;
  }

  /**
   * Sets the context encryption adapter.
   *
   * @param contextEncryptionAdapter encryption adapter.
   * @return this instance.
   */
  public AbfsInputStreamContext withEncryptionAdapter(
      ContextEncryptionAdapter contextEncryptionAdapter){
    this.contextEncryptionAdapter = contextEncryptionAdapter;
    return this;
  }

/**
   * Sets restriction of GPS on open file.
   *
   * @param restrictGpsOnOpenFile whether to restrict GPS on open file.
   * @return this instance.
   */
  public AbfsInputStreamContext shouldRestrictGpsOnOpenFile(
          final boolean restrictGpsOnOpenFile) {
    this.restrictGpsOnOpenFile = restrictGpsOnOpenFile;
    return this;
  }

  /**
   * Finalizes and validates the context configuration.
   * <p>
   * Ensures read-ahead range is valid and aligns read-ahead block size with
   * read request size if necessary.
   *
   * @return this instance.
   */
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
    Preconditions.checkArgument(readAheadRange > 0,
            "Read ahead range should be greater than 0");
    return this;
  }

  /** @return configured read buffer size. */
  public int getReadBufferSize() {
    return readBufferSize;
  }

  /** @return read-ahead queue depth. */
  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  /** @return whether out-of-band appends are tolerated. */
  public boolean isTolerateOobAppends() {
    return tolerateOobAppends;
  }

  /** @return whether read-ahead is enabled. */
  public boolean isReadAheadEnabled() {
    return isReadAheadEnabled;
  }

  /** @return whether read-ahead V2 is enabled. */
  public boolean isReadAheadV2Enabled() {
    return isReadAheadV2Enabled;
  }

  /** @return read-ahead range. */
  public int getReadAheadRange() {
    return readAheadRange;
  }

  /** @return stream statistics collector. */
  public AbfsInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /** @return whether small files should be read completely. */
  public boolean readSmallFilesCompletely() {
    return this.readSmallFilesCompletely;
  }

  /** @return whether footer read optimization is enabled. */
  public boolean optimizeFooterRead() {
    return this.optimizeFooterRead;
  }

  /** @return footer read buffer size. */
  public int getFooterReadBufferSize() {
    return footerReadBufferSize;
  }

  /** @return whether the configured buffer size is always used. */
  public boolean shouldReadBufferSizeAlways() {
    return alwaysReadBufferSize;
  }

  /** @return read-ahead block size. */
  public int getReadAheadBlockSize() {
    return readAheadBlockSize;
  }

  /** @return whether restrictGpsOnOpenFile is enabled. */
  public boolean shouldRestrictGpsOnOpenFile() {
    return this.restrictGpsOnOpenFile;
  }

  /** @return whether buffered pread is disabled. */
  public boolean isBufferedPreadDisabled() {
    return bufferedPreadDisabled;
  }

  /** @return filesystem back reference. */
  public BackReference getFsBackRef() {
    return fsBackRef;
  }

  /** @return context encryption adapter. */
  public ContextEncryptionAdapter getEncryptionAdapter() {
    return contextEncryptionAdapter;
  }
}
