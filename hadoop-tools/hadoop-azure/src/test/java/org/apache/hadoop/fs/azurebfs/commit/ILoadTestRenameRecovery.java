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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;

import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_AVAILABLE;
import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ABFS_IO_RATE_LIMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConfig.createCloseableTaskSubmitter;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.getEtag;
import static org.apache.hadoop.util.functional.FutureIO.awaitAllFutures;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * HADOOP-19093: real load test to generate rename failures.
 * This test is designed to overload the storage account so
 * will run up tangible costs as well as interfere with any
 * other use of the containers.
 */
public class ILoadTestRenameRecovery extends AbstractAbfsIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsManifestStoreOperations.class);

  /**
   * Time to sleep between checks for tasks to complete.
   */
  public static final Duration SLEEP_INTERVAL = Duration.ofMillis(1000);

  /**
   * Number of threads to use.
   */
  private final int threadCount = 100;

  /**
   * Number of renames to attempt per thread: {@value}.
   */
  public static final int RENAMES = 100;

  /**
   * Thread number; used for paths and messages.
   */
  private final AtomicInteger threadNumber = new AtomicInteger(0);

  /**
   * Flag to indicate that the test should exit: threads must check this.
   */
  private final AtomicBoolean shouldExit = new AtomicBoolean(false);

  /**
   * Any failure.
   */
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  /**
   * How many renames were recovered from.
   */
  private final AtomicLong renameRecoveries = new AtomicLong(0);

  /**
   * Store operations through which renames are applied.
   */
  private AbfsManifestStoreOperations storeOperations;

  /**
   * Task pool submitter.
   */
  private CloseableTaskPoolSubmitter submitter;

  /**
   * Base directory for the tests.
   */
  private Path baseDir;

  /**
   * Time taken to rate limit.
   * Uses a long over duration for atomic increments.
   */
  private final AtomicLong rateLimitingTime = new AtomicLong(0);

  public ILoadTestRenameRecovery() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    getRawConfiguration()
        .setInt(FS_AZURE_ABFS_IO_RATE_LIMIT, 0);
    super.setup();
    final AzureBlobFileSystem fs = getFileSystem();
    baseDir = new Path("/" + getMethodName());
    fs.mkdirs(baseDir);
    assumeThat(fs.hasPathCapability(baseDir, ETAGS_AVAILABLE))
        .describedAs("Etags must be available in the store")
        .isTrue();
    assumeThat(fs.hasPathCapability(baseDir, ETAGS_PRESERVED_IN_RENAME))
        .describedAs("Etags must be preserved across renames")
        .isTrue();
    storeOperations = new AbfsManifestStoreOperations();
    storeOperations.bindToFileSystem(fs, baseDir);
    submitter = createCloseableTaskSubmitter(threadCount, "ILoadTestRenameRecovery");
  }

  @Override
  public void teardown() throws Exception {
    submitter.close();
    try {
      getFileSystem().delete(baseDir, true);
    } catch (IOException e) {
      LOG.warn("Failed to delete {}", baseDir, e);
    }
    super.teardown();
  }

  @Test
  public void testResilientRename() throws Throwable {

    List<Future<?>> futures = new ArrayList<>(threadCount);
    final AzureBlobFileSystem fs = getFileSystem();

    // for every worker, create a file and submit a rename worker
    for (int i = 0; i < threadCount; i++) {

      final int id = threadNumber.incrementAndGet();
      final Path source = new Path(baseDir, "source-" + id);
      final Path dest = new Path(baseDir, "dest-" + id);
      // create the file in the junit test rather than in parallel,
      // as parallel creation caused OOM on buffer allocation.
      fs.create(source, true).close();

      // submit the work
      futures.add(submitter.submit(() ->
        renameWorker(id, fs, source, dest)));
    }

    // now wait for the futures
    awaitAllFutures(futures, SLEEP_INTERVAL);
    LOG.info("Rate limiting time: {}", Duration.ofMillis(rateLimitingTime.get()));

    // throw any failure which occurred.
    if (failure.get() != null) {
      throw failure.get();
    }

    Assertions.assertThat(renameRecoveries.get())
        .describedAs("Number of recoveries")
        .isGreaterThan(0);
  }

  /**
   * Worker to repeatedly rename a file.
   * @param id worker ID
   * @param fs filesystem
   * @param source source path
   * @param dest destination path
   */
  private void renameWorker(
      final int id,
      AzureBlobFileSystem fs, final Path source,
      final Path dest) {
    int recoveries = 0;
    Duration totalWaitTime = Duration.ZERO;
    try {
      LOG.info("Starting thread {}", id);

      int limit = RENAMES;
      final FileStatus st = fs.getFileStatus(source);

      final String etag = getEtag(st);
      Assertions.assertThat(etag)
          .describedAs("Etag of %s", st)
          .isNotNull()
          .isNotEmpty();
      FileEntry forwards = new FileEntry(source, dest, 0, etag);
      FileEntry backwards = new FileEntry(dest, source, 0, etag);
      int count = 0;
      while (!shouldExit.get() && count < limit) {
        FileEntry entry = count % 2 == 0 ? forwards : backwards;
        count++;
        final ManifestStoreOperations.CommitFileResult result =
            storeOperations.commitFile(entry);
        final boolean recovered = result.recovered();
        totalWaitTime = totalWaitTime.plus(result.getWaitTime());
        if (recovered) {
          recoveries++;
          noteRecovery(entry);
        }
        final Path target = entry.getDestPath();
        final FileStatus de = fs.getFileStatus(target);
        Assertions.assertThat(getEtag(de))
            .describedAs("Etag of %s with recovery=%s", de, recovered)
            .isNotNull()
            .isEqualTo(etag);
      }
      // clean up the files.
      // this is less efficient than a directory delete, but it helps
      // generate load.
      fs.delete(source, false);
      fs.delete(dest, false);

    } catch (Throwable e) {
      noteFailure(e);
    } finally {
      LOG.info("Thread {} exiting with recovery count of {} and total wait time of {}",
          id, recoveries, totalWaitTime);
      rateLimitingTime.addAndGet(totalWaitTime.toMillis());
    }
  }

  /**
   * Note that a recovery has been made.
   * @param entry entry which was recovered.
   */
  private void noteRecovery(final FileEntry entry) {
    LOG.info("Recovered from rename(failure) {}", entry);
    renameRecoveries.incrementAndGet();
    // we know recovery worked, so we can stop the test.
    shouldExit.set(true);
  }

  /**
   * Note a rename failure.
   * @param e exception.
   */
  private void noteFailure(final Throwable e) {
    LOG.error("Rename failed", e);
    failure.compareAndSet(null, e);
    shouldExit.set(true);
  }

}
