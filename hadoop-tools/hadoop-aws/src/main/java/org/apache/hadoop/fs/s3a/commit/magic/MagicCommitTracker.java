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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.fs.s3a.statistics.PutTrackerStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;

import static java.util.Objects.requireNonNull;

/**
 * Put tracker for Magic commits.
 * <p>Important</p>: must not directly or indirectly import a class which
 * uses any datatype in hadoop-mapreduce.
 */
@InterfaceAudience.Private
public abstract class MagicCommitTracker extends PutTracker {
  public static final Logger LOG = LoggerFactory.getLogger(
      MagicCommitTracker.class);

  private final String originalDestKey;
  private final String pendingPartKey;
  private final Path path;
  private final WriteOperationHelper writer;
  private final String bucket;
  protected static final byte[] EMPTY = new byte[0];
  private final PutTrackerStatistics trackerStatistics;

  /**
   * Magic commit tracker.
   * @param path path nominally being written to
   * @param bucket dest bucket
   * @param originalDestKey the original key, in the magic directory.
   * @param destKey key for the destination
   * @param pendingsetKey key of the pendingset file
   * @param writer writer instance to use for operations; includes audit span
   * @param trackerStatistics tracker statistics
   */
  public MagicCommitTracker(Path path,
      String bucket,
      String originalDestKey,
      String destKey,
      String pendingsetKey,
      WriteOperationHelper writer,
      PutTrackerStatistics trackerStatistics) {
    super(destKey);
    this.bucket = bucket;
    this.path = path;
    this.originalDestKey = originalDestKey;
    this.pendingPartKey = pendingsetKey;
    this.writer = writer;
    this.trackerStatistics = requireNonNull(trackerStatistics);
    LOG.info("File {} is written as magic file to path {}",
        path, destKey);
  }

  /**
   * Initialize the tracker.
   * @return true, indicating that the multipart commit must start.
   * @throws IOException any IO problem.
   */
  @Override
  public boolean initialize() throws IOException {
    return true;
  }

  /**
   * Flag to indicate that output is not visible after the stream
   * is closed.
   * @return true
   */
  @Override
  public boolean outputImmediatelyVisible() {
    return false;
  }

  /**
   * Complete operation: generate the final commit data, put it.
   * @param uploadId Upload ID
   * @param parts list of parts
   * @param bytesWritten bytes written
   * @param iostatistics nullable IO statistics
   * @return false, indicating that the commit must fail.
   * @throws IOException any IO problem.
   * @throws IllegalArgumentException bad argument
   */
  @Override
  public abstract boolean aboutToComplete(String uploadId,
      List<CompletedPart> parts,
      long bytesWritten,
      IOStatistics iostatistics)
      throws IOException;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MagicCommitTracker{");
    sb.append(", destKey=").append(getDestKey());
    sb.append(", pendingPartKey='").append(pendingPartKey).append('\'');
    sb.append(", path=").append(path);
    sb.append(", writer=").append(writer);
    sb.append('}');
    return sb.toString();
  }

  public String getOriginalDestKey() {
    return originalDestKey;
  }

  public String getPendingPartKey() {
    return pendingPartKey;
  }

  public Path getPath() {
    return path;
  }

  public String getBucket() {
    return bucket;
  }

  public WriteOperationHelper getWriter() {
    return writer;
  }

  public PutTrackerStatistics getTrackerStatistics() {
    return trackerStatistics;
  }
}
