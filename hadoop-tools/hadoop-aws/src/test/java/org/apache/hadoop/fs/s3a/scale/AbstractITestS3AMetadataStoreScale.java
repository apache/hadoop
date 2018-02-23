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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.NanoTimer;

/**
 * Test the performance of a MetadataStore.  Useful for load testing.
 * Could be separated from S3A code, but we're using the S3A scale test
 * framework for convenience.
 */
public abstract class AbstractITestS3AMetadataStoreScale extends
    S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractITestS3AMetadataStoreScale.class);

  /** Some dummy values for FileStatus contents. */
  static final long BLOCK_SIZE = 32 * 1024 * 1024;
  static final long SIZE = BLOCK_SIZE * 2;
  static final String OWNER = "bob";
  static final long ACCESS_TIME = System.currentTimeMillis();

  static final Path BUCKET_ROOT = new Path("s3a://fake-bucket/");

  /**
   * Subclasses should override this to provide the MetadataStore they which
   * to test.
   * @return MetadataStore to test against
   * @throws IOException
   */
  public abstract MetadataStore createMetadataStore() throws IOException;

  @Test
  public void testPut() throws Throwable {
    describe("Test workload of put() operations");

    // As described in hadoop-aws site docs, count parameter is used for
    // width and depth of directory tree
    int width = getConf().getInt(KEY_DIRECTORY_COUNT, DEFAULT_DIRECTORY_COUNT);
    int depth = width;

    List<PathMetadata> paths = new ArrayList<>();
    createDirTree(BUCKET_ROOT, depth, width, paths);

    long count = 1; // Some value in case we throw an exception below
    try (MetadataStore ms = createMetadataStore()) {

      try {
        count = populateMetadataStore(paths, ms);
      } finally {
        clearMetadataStore(ms, count);
      }
    }
  }

  @Test
  public void testMoves() throws Throwable {
    describe("Test workload of batched move() operations");

    // As described in hadoop-aws site docs, count parameter is used for
    // width and depth of directory tree
    int width = getConf().getInt(KEY_DIRECTORY_COUNT, DEFAULT_DIRECTORY_COUNT);
    int depth = width;

    long operations = getConf().getLong(KEY_OPERATION_COUNT,
        DEFAULT_OPERATION_COUNT);

    List<PathMetadata> origMetas = new ArrayList<>();
    createDirTree(BUCKET_ROOT, depth, width, origMetas);

    // Pre-compute source and destination paths for move() loop below
    List<Path> origPaths = metasToPaths(origMetas);
    List<PathMetadata> movedMetas = moveMetas(origMetas, BUCKET_ROOT,
        new Path(BUCKET_ROOT, "moved-here"));
    List<Path> movedPaths = metasToPaths(movedMetas);

    long count = 1; // Some value in case we throw an exception below
    try (MetadataStore ms = createMetadataStore()) {

      try {
        // Setup
        count = populateMetadataStore(origMetas, ms);

        // Main loop: move things back and forth
        describe("Running move workload");
        NanoTimer moveTimer = new NanoTimer();
        LOG.info("Running {} moves of {} paths each", operations,
            origMetas.size());
        for (int i = 0; i < operations; i++) {
          Collection<Path> toDelete;
          Collection<PathMetadata> toCreate;
          if (i % 2 == 0) {
            toDelete = origPaths;
            toCreate = movedMetas;
          } else {
            toDelete = movedPaths;
            toCreate = origMetas;
          }
          ms.move(toDelete, toCreate);
        }
        moveTimer.end();
        printTiming(LOG, "move", moveTimer, operations);
      } finally {
        // Cleanup
        clearMetadataStore(ms, count);
      }
    }
  }

  /**
   * Create a copy of given list of PathMetadatas with the paths moved from
   * src to dest.
   */
  private List<PathMetadata> moveMetas(List<PathMetadata> metas, Path src,
      Path dest) throws IOException {
    List<PathMetadata> moved = new ArrayList<>(metas.size());
    for (PathMetadata srcMeta : metas) {
      S3AFileStatus status = copyStatus((S3AFileStatus)srcMeta.getFileStatus());
      status.setPath(movePath(status.getPath(), src, dest));
      moved.add(new PathMetadata(status));
    }
    return moved;
  }

  private Path movePath(Path p, Path src, Path dest) {
    String srcStr = src.toUri().getPath();
    String pathStr = p.toUri().getPath();
    // Strip off src dir
    pathStr = pathStr.substring(srcStr.length());
    // Prepend new dest
    return new Path(dest, pathStr);
  }

  private S3AFileStatus copyStatus(S3AFileStatus status) {
    if (status.isDirectory()) {
      return new S3AFileStatus(status.isEmptyDirectory(), status.getPath(),
          status.getOwner());
    } else {
      return new S3AFileStatus(status.getLen(), status.getModificationTime(),
          status.getPath(), status.getBlockSize(), status.getOwner());
    }
  }

  /** @return number of PathMetadatas put() into MetadataStore */
  private long populateMetadataStore(Collection<PathMetadata> paths,
      MetadataStore ms) throws IOException {
    long count = 0;
    NanoTimer putTimer = new NanoTimer();
    describe("Inserting into MetadataStore");
    for (PathMetadata p : paths) {
      ms.put(p);
      count++;
    }
    putTimer.end();
    printTiming(LOG, "put", putTimer, count);
    return count;
  }

  private void clearMetadataStore(MetadataStore ms, long count)
      throws IOException {
    describe("Recursive deletion");
    NanoTimer deleteTimer = new NanoTimer();
    ms.deleteSubtree(BUCKET_ROOT);
    deleteTimer.end();
    printTiming(LOG, "delete", deleteTimer, count);
  }

  private static void printTiming(Logger log, String op, NanoTimer timer,
      long count) {
    double msec = (double)timer.duration() / 1000;
    double msecPerOp = msec / count;
    log.info(String.format("Elapsed %.2f msec. %.3f msec / %s (%d ops)", msec,
        msecPerOp, op, count));
  }

  private static S3AFileStatus makeFileStatus(Path path) throws IOException {
    return new S3AFileStatus(SIZE, ACCESS_TIME, path, BLOCK_SIZE, OWNER);
  }

  private static S3AFileStatus makeDirStatus(Path p) throws IOException {
    return new S3AFileStatus(false, p, OWNER);
  }

  private List<Path> metasToPaths(List<PathMetadata> metas) {
    List<Path> paths = new ArrayList<>(metas.size());
    for (PathMetadata meta : metas) {
      paths.add(meta.getFileStatus().getPath());
    }
    return paths;
  }

  /**
   * Recursively create a directory tree.
   * @param parent Parent dir of the paths to create.
   * @param depth How many more levels deep past parent to create.
   * @param width Number of files (and directories, if depth > 0) per directory.
   * @param paths List to add generated paths to.
   */
  private static void createDirTree(Path parent, int depth, int width,
      Collection<PathMetadata> paths) throws IOException {

    // Create files
    for (int i = 0; i < width; i++) {
      Path p = new Path(parent, String.format("file-%d", i));
      PathMetadata meta = new PathMetadata(makeFileStatus(p));
      paths.add(meta);
    }

    if (depth == 0) {
      return;
    }

    // Create directories if there is depth remaining
    for (int i = 0; i < width; i++) {
      Path dir = new Path(parent, String.format("dir-%d", i));
      PathMetadata meta = new PathMetadata(makeDirStatus(dir));
      paths.add(meta);
      createDirTree(dir, depth-1, width, paths);
    }
  }
}
