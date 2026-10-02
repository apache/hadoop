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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

/**
 * Optional FedBalance verification helpers.
 */
final class FedBalanceVerifier {

  private final DistributedFileSystem srcFs;
  private final DistributedFileSystem dstFs;
  private final Path src;
  private final Path dst;

  FedBalanceVerifier(DistributedFileSystem srcFs, DistributedFileSystem dstFs,
      Path src, Path dst) {
    this.srcFs = srcFs;
    this.dstFs = dstFs;
    this.src = src;
    this.dst = dst;
  }

  VerificationSummary verify() throws IOException {
    ContentSummary srcSummary = srcFs.getContentSummary(src);
    ContentSummary dstSummary = dstFs.getContentSummary(dst);
    return new VerificationSummary(
        getDirectoryCount(srcSummary), getDirectoryCount(dstSummary),
        getFileCount(srcSummary), getFileCount(dstSummary),
        getLength(srcSummary), getLength(dstSummary));
  }

  private long getDirectoryCount(ContentSummary summary) {
    return summary.getDirectoryCount() - summary.getSnapshotDirectoryCount();
  }

  private long getFileCount(ContentSummary summary) {
    return summary.getFileCount() - summary.getSnapshotFileCount();
  }

  private long getLength(ContentSummary summary) {
    return summary.getLength() - summary.getSnapshotLength();
  }

  static final class VerificationSummary {
    private final long srcDirs;
    private final long dstDirs;
    private final long srcFiles;
    private final long dstFiles;
    private final long srcLength;
    private final long dstLength;

    private VerificationSummary(long sourceDirs, long targetDirs,
        long sourceFiles, long targetFiles, long sourceLength,
        long targetLength) {
      this.srcDirs = sourceDirs;
      this.dstDirs = targetDirs;
      this.srcFiles = sourceFiles;
      this.dstFiles = targetFiles;
      this.srcLength = sourceLength;
      this.dstLength = targetLength;
    }

    boolean matches() {
      return srcDirs == dstDirs && srcFiles == dstFiles
          && srcLength == dstLength;
    }

    @Override
    public String toString() {
      return "srcDirs=" + srcDirs
          + ", dstDirs=" + dstDirs
          + ", srcFiles=" + srcFiles
          + ", dstFiles=" + dstFiles
          + ", srcLength=" + srcLength
          + ", dstLength=" + dstLength;
    }
  }
}
