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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Interface for bulk file delete operations.
 * <p>
 * The expectation is that the iterator-provided list of paths
 * will be batched into pages and submitted to the remote filesystem/store
 * for bulk deletion, possibly in parallel.
 * <p>
 * A remote iterator provides the list of paths to delete; all must be under
 * the base path.
 * <p>
 * The iterator may be a {@code Closeable} and if so, it will be closed on
 * completion, irrespective of the outcome.
 * <p>
 * The iterator may be an {@code IOStatisticsSource} and if so, its statistics
 * will be included in the statistics of the {@link Outcome}.
 * <p>
 * If the iterator's methods raise any exception, the delete will fail fast;
 * no more files will be submitted for deletion.
 * <p>
 * If a bulk delete call fails, then the next progress report will include
 * information about the failure; the callback can decide whether to continue
 * or not. The default callback is {@link #FAIL_FAST}, which will trigger
 * a fast failure.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDelete {

  /**
   * Initiate a bulk delete operation.
   * @param base base path for the delete; all files MUST be under this path
   * @param files iterator of files. If Closeable, it will be closed once complete
   * @return a builder for the operation
   * @throws UnsupportedOperationException not supported.
   * @throws IOException IO failure on initial builder creation.
   */
  Builder bulkDelete(Path base, RemoteIterator<Path> files)
      throws UnsupportedOperationException, IOException;

  /**
   * Builder for the operation;
   * The {@link #build()} method will initiate the operation, possibly
   * blocking, possibly in a separate thread, possibly in a pool
   * of threads.
   */
  interface Builder
      extends FSBuilder<CompletableFuture<BulkDelete.Outcome>, Builder> {

    /**
     * Add a progress callback.
     * @param deleteProgress progress callback
     * @return the builder
     */
    Builder withProgress(DeleteProgress deleteProgress);

  }

  /**
   * Path capability for bulk delete.
   */
  String CAPABILITY_BULK_DELETE = "fs.capability.bulk.delete";

  /**
   * Numeric hint about page size, "preferred" rather than "required".
   * Implementations will ignore this if it is out of their supported/preferred
   * range.
   */
  String OPT_PAGE_SIZE = "fs.option.bulkdelete.page.size";

  /**
   * Callback for progress; allows for a delete
   * to be aborted (best effort).
   * There are no guarantees as to which thread this will be called from.
   */
  interface DeleteProgress {

    /**
     * Report progress.
     * @param update update to report
     * @return true if the operation should continue, false to abort.
     */
    boolean report(ProgressReport update);
  }

  /**
   * Progress update data.
   */
  class ProgressReport {

    /**
     * Number of files deleted.
     */
    private final int deleteCount;

    /**
     * List of files which were deleted.
     */
    private final List<Path> successes;

    /**
     * List of files which failed to delete.
     * This may be empty, but will never be null.
     */
    private final List<Path> failures;

    /**
     * An exception covering at least one of the failures
     * encountered.
     */
    private final IOException exception;

    public ProgressReport(final int deleteCount,
        final List<Path> successes,
        final List<Path> failures,
        final IOException exception) {
      this.deleteCount = deleteCount;
      this.successes = successes;
      this.failures = failures;
      this.exception = exception;
    }

    public int getDeleteCount() {
      return deleteCount;
    }

    public List<Path> getSuccesses() {
      return successes;
    }

    public List<Path> getFailures() {
      return failures;
    }
  }

  /**
   * Result of a bulk delete operation.
   */
  class Outcome implements IOStatisticsSource {

    /**
     * Did the operation succeed?
     */
    private final boolean successful;

    /**
     * An exception covering at least one of the failures
     * encountered.
     */
    private final IOException exception;

    /**
     * Number of files deleted.
     */
    private final int deleteCount;

    /**
     * Number of delete pages submitted to the store.
     */
    private final int pageCount;

    /**
     * List of files which failed to delete.
     * This may be empty, but will never be null.
     * If the operation failed fast, it will not
     * include those files which were not submitted
     */
    private final List<Path> failures;

    /**
     * IO Statistics.
     * This will include any statistics supplied by
     * the iterator.
     */
    private final IOStatistics iostats;

    public Outcome(final boolean successful,
        final IOException exception,
        final int deleteCount,
        final int pageCount,
        final List<Path> failures,
        final IOStatistics iostats) {

      this.successful = successful;
      this.exception = exception;
      this.deleteCount = deleteCount;
      this.pageCount = pageCount;
      this.failures = failures;
      this.iostats = iostats;
    }

    public boolean successful() {
      return successful;
    }

    public IOException getException() {
      return exception;
    }

    public int getDeleteCount() {
      return deleteCount;
    }

    public List<Path> getFailures() {
      return failures;
    }

    @Override
    public IOStatistics getIOStatistics() {
      return iostats;
    }

    @Override
    public String toString() {
      return "Outcome{" +
          "successful=" + successful +
          ", deleteCount=" + deleteCount +
          ", failures=" + failures +
          ", iostats=" + iostats +
          '}';
    }
  }

  /**
   * A fail fast policy: if there are any failures, abort.
   * This is the default until any other progress callback
   * is set in the builder.
   */
  DeleteProgress FAIL_FAST = (report -> report.failures.isEmpty());

  /**
   * Continue if there are any failures.
   */
  DeleteProgress CONTINUE = (report -> true);
}
