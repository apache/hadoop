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

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

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
 * There is no guarantee order of execution.
 * Implementations may shuffle paths before posting requests.
 * <p>
 * Callers MUST have no expectation that parent directories will exist after the
 * operation completes; if an object store needs to explicitly look for and create
 * directory markers, that step will be omitted.
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
 * The {@link #OPT_BACKGROUND} boolean option is a hint to prioritise other work
 * over the delete; use it for background cleanup, compaction etc.
 * <p>
 * no guarantee of page size being greater than 1, or even constant through
 * the entire operation. The {@link #OPT_PAGE_SIZE} option can be used as
 * a hint.
 * Most object stores may have a maximum page size; if a larger size is requested,
 * you will always get something at or below that limit (i.e. it is not an
 * error to ask for more than the limit)
 * <p>
 * Be aware that on some stores (AWS S3) each object listed in a bulk delete counts
 * against the write IOPS limit; large page sizes are counterproductive here.
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-16823">HADOOP-16823.Large DeleteObject requests are their own Thundering Herd</a>
 * <p>
 * Progress callback: the callback may come from any thread, further work may or may
 * not be blocked during the callback's processing in application code.
 * <p>
 * If a bulk delete call fails, then the next progress report will include
 * information about the failure; the callback can decide whether to continue
 * or not. The default callback is {@link #FAIL_FAST}, which will trigger
 * a fast failure.
 * <p>
 * After a progress callback requests abort, active operations MAY continue.
 * The {@link ProgressReport#aborting} flag indicates this.
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
   * Is this a background operation?
   * If so, a lower write rate may be used so that it doesn't interfere
   * with higher priority workloads -such as through rate limiting
   * and/or the use of smaller page sizes.
   */
  String OPT_BACKGROUND = "fs.option.bulkdelete.background";

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
   * Delete progress report.
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

    /**
     * Has an abort been requested from a previous progress report?
     * If a progress report is delivered with this flag, it indicates
     * that the abort has been requested, and that this report is
     * from a page which was submitted before the abort request.
     */
    private final boolean aborting;

    public ProgressReport(final int deleteCount,
        final List<Path> successes,
        final List<Path> failures,
        final IOException exception,
        final boolean aborting) {
      this.deleteCount = deleteCount;
      this.successes = successes;
      this.failures = failures;
      this.exception = exception;
      this.aborting = aborting;
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

    public IOException getException() {
      return exception;
    }

    public boolean isAborting() {
      return aborting;
    }
  }

  /**
   * Result of a bulk delete operation.
   */
  class Outcome implements IOStatisticsSource {

    /**
     * Did the operation succeed?
     * That is: delete all files without any failures?
     */
    private final boolean successful;

    /**
     * Wast the operation aborted?
     */
    private final boolean aborted;

    /**
     * An exception covering at least one of the failures
     * encountered.
     */
    private final IOException exception;

    /**
     * Number of files deleted.
     */
    private final int deleted;

    /**
     * Number of files which failed to delete.
     */
    private final int failures;

    /**
     * Number of delete pages submitted to the store.
     */
    private final int pageCount;


    /**
     * IO Statistics.
     * This will include any statistics supplied by
     * the iterator.
     */
    private final IOStatistics iostats;

    public Outcome(final boolean successful,
        final boolean aborted,
        final IOException exception,
        final int deleted,
        final int failures,
        final int pageCount,
        final IOStatistics iostats) {

      this.successful = successful;
      this.aborted = aborted;
      this.exception = exception;
      this.deleted = deleted;
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

    public int getDeleted() {
      return deleted;
    }

    public int getFailures() {
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
          ", deleteCount=" + deleted +
          ", failures=" + failures +
          ", iostats=" + ioStatisticsToPrettyString(iostats) +
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
