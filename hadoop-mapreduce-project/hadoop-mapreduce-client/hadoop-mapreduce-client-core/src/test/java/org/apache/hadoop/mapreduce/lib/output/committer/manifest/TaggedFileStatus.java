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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;

import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * File Status with etag derived from the filename, if not explicitly set.
 */
public final class TaggedFileStatus extends FileStatus implements EtagSource {

  private final String etag;

  public TaggedFileStatus(final long length,
      final boolean isdir,
      final int blockReplication,
      final long blocksize,
      final long modificationTime,
      final Path path,
      final String etag) {
    super(length, isdir, blockReplication, blocksize, modificationTime, path);
    this.etag = etag;
  }

  public TaggedFileStatus(final FileStatus other, final String etag) throws IOException {
    super(other);
    this.etag = etag;
  }

  @Override
  public String getEtag() {
    return etag;
  }
}
