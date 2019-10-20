/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.util.DurationInfo;

/**
 * Note that this is used for copying single file from source to destination.
 * It can copy across buckets provided relevant permissions are available.
 * This can be enhanced later to support directory level copy with
 * multiple threads.
 */
public class CopyOperation extends ExecutingStoreOperation<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CopyOperation.class);

  private S3AFileStatus srcStatus;

  private final URI dstURI;
  private final URI srcURI;

  private OperationCallbacks callbacks;

  /**
   * Copy operation constructor.
   *
   * @param context store context
   * @param srcStatus pre-fetched source status
   * @param srcURI destination file location
   * @param dstURI destination file location
   * @param callbacks callback provider
   */
  public CopyOperation(final StoreContext context,
      final S3AFileStatus srcStatus,
      final URI srcURI,
      final URI dstURI,
      final OperationCallbacks callbacks) {

    super(context);
    this.srcStatus = srcStatus;
    this.srcURI = srcURI;
    this.dstURI = dstURI;
    this.callbacks = callbacks;
  }

  /**
   * Validations for copying file from source to destination.
   *
   * @throws IllegalArgumentException path validation
   * @throws IOException any other S3 issue
   */
  private void verifyCopyConditions()
      throws IllegalArgumentException, IOException {

    // Source should not be a directory
    if (srcStatus.isDirectory()) {
      throw new IllegalArgumentException(
          srcStatus.getPath().toUri() + " can not be a directory");
    }

    // Destination should have its parent directory
    Path dstPath = new Path(dstURI);
    FileSystem dstFS =
        dstPath.getFileSystem(getStoreContext().getConfiguration());
    if (!dstFS.exists(dstPath.getParent())) {
      throw new IllegalArgumentException(dstPath.getParent() + " not present");
    }
    try {
      FileStatus dstStatus = dstFS.getFileStatus(dstPath);
      if (dstStatus.isDirectory()) {
        throw new IllegalArgumentException(
            dstURI + " does not contain filename");
      }
    } catch (FileNotFoundException fnfe) {
      // destination file is not present. Ok to write.
    }

  }

  @Retries.RetryTranslated
  public Long execute() throws IOException {
    executeOnlyOnce();
    copyFile();
    return srcStatus.getLen();
  }

  /**
   * Copy a file to destination
   *
   * @throws IOException failure
   */
  protected void copyFile() throws IOException {
    verifyCopyConditions();
    S3ObjectAttributes sourceAttributes =
        callbacks.createObjectAttributes(srcStatus);
    S3AReadOpContext readContext = callbacks.createReadContext(srcStatus);
    LOG.debug("copy: copying file {} to {}", srcStatus.getPath().toUri(),
        dstURI);
    // copy
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Copy file from %s to %s (length=%d)", srcURI, dstURI.toString(),
        srcStatus.getLen())) {
      callbacks.copyFile(srcURI, dstURI, sourceAttributes, readContext);
    }
  }
}
