/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.commit.ops;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.tosfs.commit.Pending;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PendingOps will revert, abort or commit the given {@link Pending} commit.
 */
public class RawPendingOps implements PendingOps {
  private static final Logger LOG = LoggerFactory.getLogger(RawPendingOps.class);

  private final ObjectStorage storage;

  /**
   * Constructor for {@link PendingOpsFactory} to reflect a new instance.
   *
   * @param fs       the file system.
   * @param storage  the object storage.
   */
  public RawPendingOps(FileSystem fs, ObjectStorage storage) {
    this.storage = storage;
  }

  public void revert(Pending commit) {
    LOG.info("Revert the commit by deleting the object key - {}", commit);
    storage.delete(commit.destKey());
  }

  public void abort(Pending commit) {
    LOG.info("Abort the commit by aborting multipart upload - {}", commit);
    storage.abortMultipartUpload(commit.destKey(), commit.uploadId());
  }

  public void commit(Pending commit) {
    LOG.info("Commit by completing the multipart uploads - {}", commit);
    storage.completeUpload(commit.destKey(), commit.uploadId(), commit.parts());
  }
}
