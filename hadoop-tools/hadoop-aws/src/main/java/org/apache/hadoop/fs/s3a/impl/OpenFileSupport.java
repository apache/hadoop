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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FSBuilderSupport;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.select.InternalSelectConstants;
import org.apache.hadoop.fs.s3a.select.SelectConstants;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_END;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_START;
import static org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys;
import static org.apache.hadoop.fs.s3a.Constants.ASYNC_DRAIN_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Helper class for openFile() logic, especially processing file status
 * args and length/etag/versionID.
 * <p>
 *  This got complex enough it merited removal from S3AFileSystem -which
 *  also permits unit testing.
 * </p>
 * <p>
 *   The default values are those from the FileSystem configuration.
 *   in openFile(), they can all be changed by specific options;
 *   in FileSystem.open(path, buffersize) only the buffer size is
 *   set.
 * </p>
 */
public class OpenFileSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenFileSupport.class);

  /**
   * For use when a value of an split/file length is unknown.
   */
  private static final int LENGTH_UNKNOWN = -1;

  /**  Default change detection policy. */
  private final ChangeDetectionPolicy changePolicy;

  /** Default read ahead range. */
  private final long defaultReadAhead;

  /** Username. */
  private final String username;

  /** Default buffer size. */
  private final int defaultBufferSize;

  /**
   * Threshold for stream reads to switch to
   * asynchronous draining.
   */
  private final long defaultAsyncDrainThreshold;

  /**
   * Default input policy; may be overridden in
   * {@code openFile()}.
   */
  private final S3AInputPolicy defaultInputPolicy;

  /**
   * Instantiate with the default options from the filesystem.
   * @param changePolicy change detection policy
   * @param defaultReadAhead read ahead range
   * @param username username
   * @param defaultBufferSize buffer size
   * @param defaultAsyncDrainThreshold drain threshold
   * @param defaultInputPolicy input policy
   */
  public OpenFileSupport(
      final ChangeDetectionPolicy changePolicy,
      final long defaultReadAhead,
      final String username,
      final int defaultBufferSize,
      final long defaultAsyncDrainThreshold,
      final S3AInputPolicy defaultInputPolicy) {
    this.changePolicy = changePolicy;
    this.defaultReadAhead = defaultReadAhead;
    this.username = username;
    this.defaultBufferSize = defaultBufferSize;
    this.defaultAsyncDrainThreshold = defaultAsyncDrainThreshold;
    this.defaultInputPolicy = defaultInputPolicy;
  }

  public ChangeDetectionPolicy getChangePolicy() {
    return changePolicy;
  }

  public long getDefaultReadAhead() {
    return defaultReadAhead;
  }

  public int getDefaultBufferSize() {
    return defaultBufferSize;
  }

  public long getDefaultAsyncDrainThreshold() {
    return defaultAsyncDrainThreshold;
  }

  /**
   * Propagate the default options to the operation context
   * being built up.
   * @param roc context
   * @return the context
   */
  public S3AReadOpContext applyDefaultOptions(S3AReadOpContext roc) {
    return roc
        .withInputPolicy(defaultInputPolicy)
        .withChangeDetectionPolicy(changePolicy)
        .withAsyncDrainThreshold(defaultAsyncDrainThreshold)
        .withReadahead(defaultReadAhead);
  }

  /**
   * Prepare to open a file from the openFile parameters.
   * @param path path to the file
   * @param parameters open file parameters from the builder.
   * @param blockSize for fileStatus
   * @return open file options
   * @throws IOException failure to resolve the link.
   * @throws IllegalArgumentException unknown mandatory key
   */
  @SuppressWarnings("ChainOfInstanceofChecks")
  public OpenFileInformation prepareToOpenFile(
      final Path path,
      final OpenFileParameters parameters,
      final long blockSize) throws IOException {
    Configuration options = parameters.getOptions();
    Set<String> mandatoryKeys = parameters.getMandatoryKeys();
    String sql = options.get(SelectConstants.SELECT_SQL, null);
    boolean isSelect = sql != null;
    // choice of keys depends on open type
    if (isSelect) {
      // S3 Select call adds a large set of supported mandatory keys
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalSelectConstants.SELECT_OPTIONS,
          "for " + path + " in S3 Select operation");
    } else {
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalConstants.S3A_OPENFILE_KEYS,
          "for " + path + " in non-select file I/O");
    }

    // where does a read end?
    long fileLength = LENGTH_UNKNOWN;

    // was a status passed in via a withStatus() invocation in
    // the builder API?
    FileStatus providedStatus = parameters.getStatus();
    S3AFileStatus fileStatus = null;
    if (providedStatus != null) {
      // there's a file status

      // make sure the file name matches -the rest of the path
      // MUST NOT be checked.
      Path providedStatusPath = providedStatus.getPath();
      checkArgument(path.getName().equals(providedStatusPath.getName()),
          "Filename mismatch between file being opened %s and"
              + " supplied filestatus %s",
          path, providedStatusPath);

      // make sure the status references a file
      if (providedStatus.isDirectory()) {
        throw new FileNotFoundException(
            "Supplied status references a directory " + providedStatus);
      }
      // build up the values
      long len = providedStatus.getLen();
      long modTime = providedStatus.getModificationTime();
      String versionId;
      String eTag;
      // can use this status to skip our own probes,
      LOG.debug("File was opened with a supplied FileStatus;"
              + " skipping getFileStatus call in open() operation: {}",
          providedStatus);

      // what type is the status (and hence: what information does it contain?)
      if (providedStatus instanceof S3AFileStatus) {
        // is it an S3AFileSystem status?
        S3AFileStatus st = (S3AFileStatus) providedStatus;
        versionId = st.getVersionId();
        eTag = st.getEtag();
      } else if (providedStatus instanceof S3ALocatedFileStatus) {

        //  S3ALocatedFileStatus instance may supply etag and version.
        S3ALocatedFileStatus st = (S3ALocatedFileStatus) providedStatus;
        versionId = st.getVersionId();
        eTag = st.getEtag();
      } else {
        // it is another type.
        // build a status struct without etag or version.
        LOG.debug("Converting file status {}", providedStatus);
        versionId = null;
        eTag = null;
      }
      // Construct a new file status with the real path of the file.
      fileStatus = new S3AFileStatus(
          len,
          modTime,
          path,
          blockSize,
          username,
          eTag,
          versionId);
      // set the end of the read to the file length
      fileLength = fileStatus.getLen();
    }
    FSBuilderSupport builderSupport = new FSBuilderSupport(options);
    // determine start and end of file.
    long splitStart = builderSupport.getPositiveLong(FS_OPTION_OPENFILE_SPLIT_START, 0);

    // split end
    long splitEnd = builderSupport.getLong(
        FS_OPTION_OPENFILE_SPLIT_END, LENGTH_UNKNOWN);

    if (splitStart > 0 && splitStart > splitEnd) {
      LOG.warn("Split start {} is greater than split end {}, resetting",
          splitStart, splitEnd);
      splitStart = 0;
    }

    // read end is the open file value
    fileLength = builderSupport.getPositiveLong(FS_OPTION_OPENFILE_LENGTH, fileLength);

    // if the read end has come from options, use that
    // in creating a file status
    if (fileLength >= 0 && fileStatus == null) {
      fileStatus = createStatus(path, fileLength, blockSize);
    }

    // Build up the input policy.
    // seek policy from default, s3a opt or standard option
    // read from the FS standard option.
    Collection<String> policies =
        options.getStringCollection(FS_OPTION_OPENFILE_READ_POLICY);
    if (policies.isEmpty()) {
      // fall back to looking at the S3A-specific option.
      policies = options.getStringCollection(INPUT_FADVISE);
    }

    return new OpenFileInformation()
        .withS3Select(isSelect)
        .withSql(sql)
        .withAsyncDrainThreshold(
            builderSupport.getPositiveLong(ASYNC_DRAIN_THRESHOLD,
                defaultReadAhead))
        .withBufferSize(
            (int)builderSupport.getPositiveLong(
                FS_OPTION_OPENFILE_BUFFER_SIZE, defaultBufferSize))
        .withChangePolicy(changePolicy)
        .withFileLength(fileLength)
        .withInputPolicy(
            S3AInputPolicy.getFirstSupportedPolicy(policies, defaultInputPolicy))
        .withReadAheadRange(
            builderSupport.getPositiveLong(READAHEAD_RANGE, defaultReadAhead))
        .withSplitStart(splitStart)
        .withSplitEnd(splitEnd)
        .withStatus(fileStatus)
        .build();

  }

  /**
   * Create a minimal file status.
   * @param path path
   * @param length file length/read end
   * @param blockSize block size
   * @return a new status
   */
  private S3AFileStatus createStatus(Path path, long length, long blockSize) {
    return new S3AFileStatus(
        length,
        0,
        path,
        blockSize,
        username,
        null,
        null);
  }

  /**
   * Open a simple file, using all the default
   * options.
   * @return the parameters needed to open a file through
   * {@code open(path, bufferSize)}.
   * @param bufferSize  buffer size
   */
  public OpenFileInformation openSimpleFile(final int bufferSize) {
    return new OpenFileInformation()
        .withS3Select(false)
        .withAsyncDrainThreshold(defaultAsyncDrainThreshold)
        .withBufferSize(bufferSize)
        .withChangePolicy(changePolicy)
        .withFileLength(LENGTH_UNKNOWN)
        .withInputPolicy(defaultInputPolicy)
        .withReadAheadRange(defaultReadAhead)
        .withSplitStart(0)
        .withSplitEnd(LENGTH_UNKNOWN)
        .build();
  }

  @Override
  public String toString() {
    return "OpenFileSupport{" +
        "changePolicy=" + changePolicy +
        ", defaultReadAhead=" + defaultReadAhead +
        ", defaultBufferSize=" + defaultBufferSize +
        ", defaultAsyncDrainThreshold=" + defaultAsyncDrainThreshold +
        ", defaultInputPolicy=" + defaultInputPolicy +
        '}';
  }

  /**
   * The information on a file needed to open it.
   */
  public static final class OpenFileInformation {

    /** Is this SQL? */
    private boolean isS3Select;

    /** File status; may be null. */
    private S3AFileStatus status;

    /** SQL string if this is a SQL select file. */
    private String sql;

    /** Active input policy. */
    private S3AInputPolicy inputPolicy;

    /** Change detection policy. */
    private ChangeDetectionPolicy changePolicy;

    /** Read ahead range. */
    private long readAheadRange;

    /** Buffer size. Currently ignored. */
    private int bufferSize;

    /**
     * Where does the read start from. 0 unless known.
     */
    private long splitStart;

    /**
     * What is the split end?
     * Negative if not known.
     */
    private long splitEnd = -1;

    /**
     * What is the file length?
     * Negative if not known.
     */
    private long fileLength = -1;

    /**
     * Threshold for stream reads to switch to
     * asynchronous draining.
     */
    private long asyncDrainThreshold;

    /**
     * Constructor.
     */
    public OpenFileInformation() {
    }

    /**
     * Build.
     * @return this object
     */
    public OpenFileInformation build() {
      return this;
    }

    public boolean isS3Select() {
      return isS3Select;
    }

    public S3AFileStatus getStatus() {
      return status;
    }

    public String getSql() {
      return sql;
    }

    public S3AInputPolicy getInputPolicy() {
      return inputPolicy;
    }

    public ChangeDetectionPolicy getChangePolicy() {
      return changePolicy;
    }

    public long getReadAheadRange() {
      return readAheadRange;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public long getSplitStart() {
      return splitStart;
    }

    public long getSplitEnd() {
      return splitEnd;
    }

    @Override
    public String toString() {
      return "OpenFileInformation{" +
          "isSql=" + isS3Select +
          ", status=" + status +
          ", sql='" + sql + '\'' +
          ", inputPolicy=" + inputPolicy +
          ", changePolicy=" + changePolicy +
          ", readAheadRange=" + readAheadRange +
          ", splitStart=" + splitStart +
          ", splitEnd=" + splitEnd +
          ", bufferSize=" + bufferSize +
          ", drainThreshold=" + asyncDrainThreshold +
          '}';
    }

    /**
     * Get the file length.
     * @return the file length; -1 if not known.
     */
    public long getFileLength() {
      return fileLength;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withS3Select(final boolean value) {
      isS3Select = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withStatus(final S3AFileStatus value) {
      status = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withSql(final String value) {
      sql = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withInputPolicy(final S3AInputPolicy value) {
      inputPolicy = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withChangePolicy(final ChangeDetectionPolicy value) {
      changePolicy = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withReadAheadRange(final long value) {
      readAheadRange = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withBufferSize(final int value) {
      bufferSize = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withSplitStart(final long value) {
      splitStart = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withSplitEnd(final long value) {
      splitEnd = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withFileLength(final long value) {
      fileLength = value;
      return this;
    }

    /**
     * Set builder value.
     * @param value new value
     * @return the builder
     */
    public OpenFileInformation withAsyncDrainThreshold(final long value) {
      asyncDrainThreshold = value;
      return this;
    }

    /**
     * Propagate the options to the operation context
     * being built up.
     * @param roc context
     * @return the context
     */
    public S3AReadOpContext applyOptions(S3AReadOpContext roc) {
      return roc
          .withInputPolicy(inputPolicy)
          .withChangeDetectionPolicy(changePolicy)
          .withAsyncDrainThreshold(asyncDrainThreshold)
          .withReadahead(readAheadRange);
    }

  }

}
