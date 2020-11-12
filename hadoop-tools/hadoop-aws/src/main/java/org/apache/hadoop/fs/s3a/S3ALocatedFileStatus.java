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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link LocatedFileStatus} extended to also carry ETag and object version ID.
 */
public class S3ALocatedFileStatus extends LocatedFileStatus {

  private static final long serialVersionUID = 3597192103662929338L;

  private final String eTag;
  private final String versionId;

  private final Tristate isEmptyDirectory;

  public S3ALocatedFileStatus(S3AFileStatus status, BlockLocation[] locations) {
    super(checkNotNull(status), locations);
    this.eTag = status.getETag();
    this.versionId = status.getVersionId();
    isEmptyDirectory = status.isEmptyDirectory();
  }

  public String getETag() {
    return eTag;
  }

  public String getVersionId() {
    return versionId;
  }

  // equals() and hashCode() overridden to avoid FindBugs warning.
  // Base implementation is equality on Path only, which is still appropriate.

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Generate an S3AFileStatus instance, including etag and
   * version ID, if present.
   */
  public S3AFileStatus toS3AFileStatus() {
    return new S3AFileStatus(
        getPath(),
        isDirectory(),
        isEmptyDirectory,
        getLen(),
        getModificationTime(),
        getBlockSize(),
        getOwner(),
        getETag(),
        getVersionId());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        super.toString());
    sb.append("[eTag='").
        append(eTag != null ? eTag : "")
        .append('\'');
    sb.append(", versionId='")
        .append(versionId != null ? versionId: "")
        .append('\'');
    sb.append(']');
    return sb.toString();
  }
}
