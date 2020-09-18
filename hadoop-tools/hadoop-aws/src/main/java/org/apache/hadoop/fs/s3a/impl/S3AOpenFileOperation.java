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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.select.InternalSelectConstants;
import org.apache.hadoop.fs.s3a.select.SelectConstants;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;

/**
 * Helper class for openFile() logic, especially processing file status
 * args and length/etag/versionID.
 * <p></p>
 * This got complex enough it merited removal from S3AFS -which
 * also permits unit testing.
 */
public class S3AOpenFileOperation extends AbstractStoreOperation {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AOpenFileOperation.class);

  private final S3AInputPolicy inputPolicy;

  private final ChangeDetectionPolicy changePolicy;

  private final long readAheadRange;

  private final String username;

  /**
   * Simple file information is created in advance as it is always
   * the same.
   */
  private final OpenFileInformation simpleFileInformation;

  /**
   * Instantiate with the default options from the filesystem.
   * @param inputPolicy input policy
   * @param changePolicy change detection policy
   * @param readAheadRange read ahead range
   * @param username username
   */
  public S3AOpenFileOperation(
      final S3AInputPolicy inputPolicy,
      final ChangeDetectionPolicy changePolicy,
      final long readAheadRange,
      final String username) {
    super(null);
    this.inputPolicy = inputPolicy;
    this.changePolicy = changePolicy;
    this.readAheadRange = readAheadRange;
    this.username = username;

    simpleFileInformation = new OpenFileInformation(false, null, null,
        inputPolicy, changePolicy, readAheadRange);
  }


  /**
   * The information on a file needed to open it.
   */
  public static final class OpenFileInformation {

    /** Is this SQL? */
    private final boolean isSql;

    /** File status; may be null. */
    private final S3AFileStatus status;

    /** SQL string if this is a SQL select file. */
    private final String sql;

    /** Active input policy. */
    private final S3AInputPolicy inputPolicy;

    /** Change detection policy. */
    private final ChangeDetectionPolicy changePolicy;

    /** read ahead range. */
    private final long readAheadRange;

    /**
     * Constructor.
     */
    private OpenFileInformation(final boolean isSql,
        final S3AFileStatus status,
        final String sql,
        final S3AInputPolicy inputPolicy,
        final ChangeDetectionPolicy changePolicy,
        final long readAheadRange) {
      this.isSql = isSql;
      this.status = status;
      this.sql = sql;
      this.inputPolicy = inputPolicy;
      this.changePolicy = changePolicy;
      this.readAheadRange = readAheadRange;
    }

    public boolean isSql() {
      return isSql;
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

    @Override
    public String toString() {
      return "OpenFileInformation{" +
          "isSql=" + isSql +
          ", status=" + status +
          ", sql='" + sql + '\'' +
          ", inputPolicy=" + inputPolicy +
          ", changePolicy=" + changePolicy +
          ", readAheadRange=" + readAheadRange +
          '}';
    }
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
    FileStatus providedStatus = parameters.getStatus();
    S3AFileStatus fileStatus;
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
      if (providedStatus instanceof S3AFileStatus) {
        S3AFileStatus st = (S3AFileStatus) providedStatus;
        versionId = st.getVersionId();
        eTag = st.getETag();
      } else if (providedStatus instanceof S3ALocatedFileStatus) {
        //  S3ALocatedFileStatus instance may supply etag and version.
        S3ALocatedFileStatus st
            = (S3ALocatedFileStatus) providedStatus;
        versionId = st.getVersionId();
        eTag = st.getETag();
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
    } else if (options.get(FS_OPTION_OPENFILE_LENGTH) != null) {
      // build a minimal S3A FileStatus From the input length alone.
      // this is all we actually need.
      long length = options.getLong(FS_OPTION_OPENFILE_LENGTH, 0);
      LOG.debug("Fixing length of file to read {} as {}", path, length);
      fileStatus = new S3AFileStatus(
          length,
          0,
          path,
          blockSize,
          username,
          null,
          null);
    } else {
      // neither a file status nor a file length
      fileStatus = null;
    }
    // seek policy from default, s3a opt or standard option
    String policy1 = options.get(INPUT_FADVISE, inputPolicy.toString());
    String policy2 = options.get(FS_OPTION_OPENFILE_FADVISE, policy1);
    S3AInputPolicy policy = S3AInputPolicy.getPolicy(policy2);

    // readahead range
    long ra = options.getLong(READAHEAD_RANGE, readAheadRange);
    return new OpenFileInformation(isSelect, fileStatus, sql, policy,
        changePolicy, ra);
  }

  /**
   * Open a simple file.
   * @return the parameters needed to open a file through open().
   */
  public OpenFileInformation openSimpleFile() {
    return simpleFileInformation;
  }

}
