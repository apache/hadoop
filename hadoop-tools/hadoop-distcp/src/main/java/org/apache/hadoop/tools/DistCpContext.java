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

package org.apache.hadoop.tools;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import java.util.List;
import java.util.Set;

/**
 * This is the context of the distcp at runtime.
 *
 * It has the immutable {@link DistCpOptions} and mutable runtime status.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DistCpContext {
  private final DistCpOptions options;

  /** The source paths can be set at runtime via snapshots. */
  private List<Path> sourcePaths;

  /** This is a derived field, it's initialized in the beginning of distcp. */
  private boolean targetPathExists = true;

  /** Indicate that raw.* xattrs should be preserved if true. */
  private boolean preserveRawXattrs = false;

  public DistCpContext(DistCpOptions options) {
    this.options = options;
    this.sourcePaths = options.getSourcePaths();
  }

  public void setSourcePaths(List<Path> sourcePaths) {
    this.sourcePaths = sourcePaths;
  }

  /**
   * @return the sourcePaths. Please note this method does not directly delegate
   * to the {@link #options}.
   */
  public List<Path> getSourcePaths() {
    return sourcePaths;
  }

  public Path getSourceFileListing() {
    return options.getSourceFileListing();
  }

  public Path getTargetPath() {
    return options.getTargetPath();
  }

  public boolean shouldAtomicCommit() {
    return options.shouldAtomicCommit();
  }

  public boolean shouldSyncFolder() {
    return options.shouldSyncFolder();
  }

  public boolean shouldDeleteMissing() {
    return options.shouldDeleteMissing();
  }

  public boolean shouldIgnoreFailures() {
    return options.shouldIgnoreFailures();
  }

  public boolean shouldOverwrite() {
    return options.shouldOverwrite();
  }

  public boolean shouldAppend() {
    return options.shouldAppend();
  }

  public boolean shouldSkipCRC() {
    return options.shouldSkipCRC();
  }

  public boolean shouldBlock() {
    return options.shouldBlock();
  }

  public boolean shouldUseDiff() {
    return options.shouldUseDiff();
  }

  public boolean shouldUseRdiff() {
    return options.shouldUseRdiff();
  }

  public boolean shouldUseSnapshotDiff() {
    return options.shouldUseSnapshotDiff();
  }

  public String getFromSnapshot() {
    return options.getFromSnapshot();
  }

  public String getToSnapshot() {
    return options.getToSnapshot();
  }

  public final String getFiltersFile() {
    return options.getFiltersFile();
  }

  public int getNumListstatusThreads() {
    return options.getNumListstatusThreads();
  }

  public int getMaxMaps() {
    return options.getMaxMaps();
  }

  public float getMapBandwidth() {
    return options.getMapBandwidth();
  }

  public Set<FileAttribute> getPreserveAttributes() {
    return options.getPreserveAttributes();
  }

  public boolean shouldPreserve(FileAttribute attribute) {
    return options.shouldPreserve(attribute);
  }

  public boolean shouldPreserveRawXattrs() {
    return preserveRawXattrs;
  }

  public void setPreserveRawXattrs(boolean preserveRawXattrs) {
    this.preserveRawXattrs = preserveRawXattrs;
  }

  public Path getAtomicWorkPath() {
    return options.getAtomicWorkPath();
  }

  public Path getLogPath() {
    return options.getLogPath();
  }

  public String getCopyStrategy() {
    return options.getCopyStrategy();
  }

  public int getBlocksPerChunk() {
    return options.getBlocksPerChunk();
  }

  public boolean shouldUseIterator() {
    return options.shouldUseIterator();
  }

  public final boolean splitLargeFile() {
    return options.getBlocksPerChunk() > 0;
  }

  public int getCopyBufferSize() {
    return options.getCopyBufferSize();
  }

  public boolean shouldDirectWrite() {
    return options.shouldDirectWrite();
  }

  public void setTargetPathExists(boolean targetPathExists) {
    this.targetPathExists = targetPathExists;
  }

  public boolean isTargetPathExists() {
    return targetPathExists;
  }

  public void appendToConf(Configuration conf) {
    options.appendToConf(conf);
  }

  @Override
  public String toString() {
    return options.toString() +
        ", sourcePaths=" + sourcePaths +
        ", targetPathExists=" + targetPathExists +
        ", preserveRawXattrs" + preserveRawXattrs;
  }

}
