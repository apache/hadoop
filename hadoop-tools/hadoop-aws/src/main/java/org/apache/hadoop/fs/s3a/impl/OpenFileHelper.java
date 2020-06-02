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

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.select.InternalSelectConstants;
import org.apache.hadoop.fs.s3a.select.SelectConstants;

import static org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys;
import static org.apache.hadoop.fs.s3a.Constants.OPEN_OPTION_LENGTH;

/**
 * Helper class for openFile() logic, especially processing file status
 * args and length/etag/versionID.
 * This has got complex enough it merited removal from S3AFS; there is
 * an interface to call back into that where needed.
 */
public class OpenFileHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenFileHelper.class);

  /**
   * Prepare to open a file from the openFile parameters.
   * @param path path to the file
   * @param parameters open file parameters from the builder.
   * @param username username for fileStatus
   * @param blockSize for fileStatus
   * @return triple of (isSql, fileStatus?, sql?)
   * @throws IOException failure to resolve the link.
   * @throws IllegalArgumentException unknown mandatory key
   */
  public Triple<Boolean, S3AFileStatus, String> prepareToOpenFile(
      final Path path,
      final OpenFileParameters parameters,
      final String username,
      final long blockSize) throws IOException {
    Configuration options = parameters.getOptions();
    Set<String> mandatoryKeys = parameters.getMandatoryKeys();
    String sql = options.get(SelectConstants.SELECT_SQL, null);
    boolean isSelect = sql != null;
    // choice of keys depends on open type
    if (isSelect) {
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalSelectConstants.SELECT_OPTIONS,
          "for " + path + " in S3 Select operation");
    } else {
      rejectUnknownMandatoryKeys(
          mandatoryKeys,
          InternalConstants.STANDARD_OPENFILE_KEYS,
          "for " + path + " in non-select file I/O");
    }
    FileStatus providedStatus = parameters.getStatus();
    S3AFileStatus fileStatus;
    if (providedStatus != null) {
      long len = providedStatus.getLen();
      long modTime = providedStatus.getModificationTime();
      String versionId;
      String eTag;
      // can use this status to skip our own probes,
      LOG.debug("File was opened with a supplied FileStatus;"
          + " skipping getFileStatus call in open() operation: {}",

          providedStatus);
      if (providedStatus instanceof S3AFileStatus) {
        // including etag and version.
        S3AFileStatus st = (S3AFileStatus) providedStatus;
        versionId = st.getVersionId();
        eTag = st.getETag();
      } else if (providedStatus instanceof S3ALocatedFileStatus) {
        LOG.debug("File was opened with a supplied S3ALocatedFileStatus;"
                + " skipping getFileStatus call in open() operation: {}",
            providedStatus);
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
      fileStatus = new S3AFileStatus(
          len,
          modTime,
          path,
          blockSize,
          username,
          eTag,
          versionId);
    } else if (options.get(OPEN_OPTION_LENGTH) != null) {
      // build a minimal S3A FileStatus From the input length alone.
      // this is all we actually need.
      long length = options.getLong(OPEN_OPTION_LENGTH, 0);
      LOG.debug("Fixing length of file to read {} as {}", path, length);
      fileStatus = new S3AFileStatus(
          length,
          0,
          path,
          blockSize,
          username,
          null, null);
    } else {
      fileStatus = null;
    }
    return Triple.of(isSelect, fileStatus, sql);
  }

}
