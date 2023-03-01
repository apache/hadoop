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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * GetContentSummary operation.
 *
 * It is optimized for s3 and performs a deep tree listing,
 * inferring directory counts from the paths returned.
 *
 * The Operation serves up IOStatistics; this counts
 * the cost of all the list operations, but not the
 * initial HEAD probe to see if the path is a file.
 */
public class GetContentSummaryOperation extends
    ExecutingStoreOperation<ContentSummary> implements IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      GetContentSummaryOperation.class);

  /**
   * Directory to scan.
   */
  private final Path path;

  /**
   * Callbacks to the store.
   */
  private final GetContentSummaryCallbacks callbacks;

  /**
   * IOStatistics to serve up.
   */
  private final IOStatisticsSnapshot iostatistics =
      new IOStatisticsSnapshot();

  /**
   * Constructor.
   * @param storeContext context.
   * @param path path to summarize
   * @param callbacks callbacks for S3 access.
   */
  public GetContentSummaryOperation(
      final StoreContext storeContext,
      final Path path,
      final GetContentSummaryCallbacks callbacks) {
    super(storeContext);
    this.path = path;
    this.callbacks = callbacks;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return iostatistics;
  }

  /**
   * Return the {@link ContentSummary} of a given path.
   * @return the summary.
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException failure
   */
  @Override
  @Retries.RetryTranslated
  public ContentSummary execute() throws IOException {
    FileStatus status = probePathStatusOrNull(path, StatusProbeEnum.FILE);
    if (status != null && status.isFile()) {
      // f is a file
      long length = status.getLen();
      return new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(length).build();
    }
    final ContentSummary summary = getDirSummary(path);
    // Log the IOStatistics at debug so the cost of the operation
    // can be made visible.
    LOG.debug("IOStatistics of getContentSummary({}):\n{}", path, iostatistics);
    return summary;
  }

  /**
   * Return the {@link ContentSummary} of a given directory.
   *
   * @param dir dir to scan
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   * @return the content summary
   * @throws FileNotFoundException the path does not exist
   * @throws IOException failure
   */
  public ContentSummary getDirSummary(Path dir) throws IOException {

    long totalLength = 0;
    long fileCount = 0;
    long dirCount = 1;

    RemoteIterator<S3ALocatedFileStatus> it = callbacks.listFilesIterator(dir, true);

    Set<Path> dirSet = new HashSet<>();
    Set<Path> pathsTraversed = new HashSet<>();

    while (it.hasNext()) {
      S3ALocatedFileStatus fileStatus = it.next();
      Path filePath = fileStatus.getPath();

      if (fileStatus.isDirectory() && !filePath.equals(dir)) {
        dirSet.add(filePath);
        buildDirectorySet(dirSet, pathsTraversed, dir, filePath.getParent());
      } else if (!fileStatus.isDirectory()) {
        fileCount += 1;
        totalLength += fileStatus.getLen();
        buildDirectorySet(dirSet, pathsTraversed, dir, filePath.getParent());
      }

    }

    // Add the list's IOStatistics
    iostatistics.aggregate(retrieveIOStatistics(it));

    return new ContentSummary.Builder().length(totalLength).
            fileCount(fileCount).directoryCount(dirCount + dirSet.size()).
            spaceConsumed(totalLength).build();
  }

  /***
   * This method builds the set of all directories found under the base path. We need to do this
   * because if the directory structure /a/b/c was created with a single mkdirs() call, it is
   * stored as 1 object in S3 and the list files iterator will only return a single entry /a/b/c.
   *
   * We keep track of paths traversed so far to prevent duplication of work. For eg, if we had
   * a/b/c/file-1.txt and /a/b/c/file-2.txt, we will only recurse over the complete path once
   * and won't have to do anything for file-2.txt.
   *
   * @param dirSet Set of all directories found in the path
   * @param pathsTraversed Set of all paths traversed so far
   * @param basePath Path of directory to scan
   * @param parentPath Parent path of the current file/directory in the iterator
   */
  private void buildDirectorySet(Set<Path> dirSet, Set<Path> pathsTraversed, Path basePath,
      Path parentPath) {

    if (parentPath == null || pathsTraversed.contains(parentPath) || parentPath.equals(basePath)) {
      return;
    }

    dirSet.add(parentPath);

    buildDirectorySet(dirSet, pathsTraversed, basePath, parentPath.getParent());

    pathsTraversed.add(parentPath);
  }

  /**
   * Get the status of a path, downgrading FNFE to null result.
   * @param p path to probe.
   * @param probes probes to exec
   * @return the status or null
   * @throws IOException failure other than FileNotFound
   */
  private S3AFileStatus probePathStatusOrNull(final Path p,
      final Set<StatusProbeEnum> probes) throws IOException {
    try {
      return callbacks.probePathStatus(p, probes);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
  }

  /**
   * Callbacks used by the operation.
   */
  public interface GetContentSummaryCallbacks {

    /**
     * Get the status of a path.
     *
     * @param path   path to probe.
     * @param probes probes to exec
     * @return the status
     * @throws IOException failure
     */
    @Retries.RetryTranslated
    S3AFileStatus probePathStatus(Path path, Set<StatusProbeEnum> probes) throws IOException;

    /***
     * List all entries under a path.
     * @param path path.
     * @param recursive if the subdirectories need to be traversed recursively
     * @return an iterator over the listing.
     * @throws IOException failure
     */
    RemoteIterator<S3ALocatedFileStatus> listFilesIterator(Path path, boolean recursive)
        throws IOException;
  }
}
