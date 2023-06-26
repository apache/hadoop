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

package org.apache.hadoop.fs.s3a.commit.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperations;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.impl.AbstractStoreOperation;
import org.apache.hadoop.fs.s3a.impl.HeaderProcessing;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.listAndFilter;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_COMMIT_JOB;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_MATERIALIZE_FILE;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_LOAD_SINGLE_PENDING_FILE;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_STAGE_FILE_UPLOAD;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.XA_MAGIC_MARKER;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants._SUCCESS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.util.functional.RemoteIterators.cleanupRemoteIterator;

/**
 * The implementation of the various actions a committer needs.
 * This doesn't implement the protocol/binding to a specific execution engine,
 * just the operations needed to to build one.
 *
 * When invoking FS operations, it assumes that the underlying FS is
 * handling retries and exception translation: it does not attempt to
 * duplicate that work.
 *
 */
public class CommitOperations extends AbstractStoreOperation
    implements IOStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(
      CommitOperations.class);

  /**
   * Destination filesystem.
   */
  private final S3AFileSystem fs;

  /** Statistics. */
  private final CommitterStatistics statistics;

  /**
   * Write operations for the destination fs.
   */
  private final WriteOperations writeOperations;

  /**
   * Filter to find all {code .pendingset} files.
   */
  public static final PathFilter PENDINGSET_FILTER =
      path -> path.toString().endsWith(CommitConstants.PENDINGSET_SUFFIX);

  /**
   * Filter to find all {code .pending} files.
   */
  public static final PathFilter PENDING_FILTER =
      path -> path.toString().endsWith(CommitConstants.PENDING_SUFFIX);

  /**
   * Instantiate.
   * @param fs FS to bind to
   * @throws IOException failure to bind.
   */
  public CommitOperations(S3AFileSystem fs) throws IOException {
    this(requireNonNull(fs), fs.newCommitterStatistics(), "/");
  }

  /**
   * Instantiate. This creates a new audit span for
   * the commit operations.
   * @param fs FS to bind to
   * @param committerStatistics committer statistics
   * @param outputPath destination of work.
   * @throws IOException failure to bind.
   */
  public CommitOperations(S3AFileSystem fs,
      CommitterStatistics committerStatistics,
      String outputPath) throws IOException {
    super(requireNonNull(fs).createStoreContext());
    this.fs = fs;
    statistics = requireNonNull(committerStatistics);
    // create a span
    writeOperations = fs.createWriteOperationHelper(
        fs.getAuditSpanSource().createSpan(
            COMMITTER_COMMIT_JOB.getSymbol(),
            outputPath, null));
  }

  /**
   * Convert an ordered list of strings to a list of index etag parts.
   * @param tagIds list of tags
   * @return same list, now in numbered tuples
   */
  public static List<PartETag> toPartEtags(List<String> tagIds) {
    return IntStream.range(0, tagIds.size())
        .mapToObj(i -> new PartETag(i + 1, tagIds.get(i)))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "CommitOperations{" + fs.getUri() + '}';
  }

  /** @return statistics. */
  protected CommitterStatistics getStatistics() {
    return statistics;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return statistics.getIOStatistics();
  }

  /**
   * Commit the operation, throwing an exception on any failure.
   * @param commit commit to execute
   * @throws IOException on a failure
   */
  public void commitOrFail(
      final SinglePendingCommit commit) throws IOException {
    commit(commit, commit.getFilename()).maybeRethrow();
  }

  /**
   * Commit a single pending commit; exceptions are caught
   * and converted to an outcome.
   * @param commit entry to commit
   * @param origin origin path/string for outcome text
   * @return the outcome
   */
  public MaybeIOE commit(
      final SinglePendingCommit commit,
      final String origin) {
    LOG.debug("Committing single commit {}", commit);
    MaybeIOE outcome;
    String destKey = "unknown destination";
    try (DurationInfo d = new DurationInfo(LOG,
        "Committing file %s size %s",
        commit.getDestinationKey(),
        commit.getLength())) {

      commit.validate();
      destKey = commit.getDestinationKey();
      long l = trackDuration(statistics, COMMITTER_MATERIALIZE_FILE.getSymbol(),
          () -> innerCommit(commit));
      LOG.debug("Successful commit of file length {}", l);
      outcome = MaybeIOE.NONE;
      statistics.commitCompleted(commit.getLength());
    } catch (IOException e) {
      String msg = String.format("Failed to commit upload against %s: %s",
          destKey, e);
      LOG.warn(msg, e);
      outcome = new MaybeIOE(e);
      statistics.commitFailed();
    } catch (Exception e) {
      String msg = String.format("Failed to commit upload against %s," +
          " described in %s: %s", destKey, origin, e);
      LOG.warn(msg, e);
      outcome = new MaybeIOE(new PathCommitException(origin, msg, e));
      statistics.commitFailed();
    }
    return outcome;
  }

  /**
   * Inner commit operation.
   * @param commit entry to commit
   * @return bytes committed.
   * @throws IOException failure
   */
  private long innerCommit(
      final SinglePendingCommit commit) throws IOException {
    // finalize the commit
    writeOperations.commitUpload(
        commit.getDestinationKey(),
        commit.getUploadId(),
        toPartEtags(commit.getEtags()),
        commit.getLength());
    return commit.getLength();
  }

  /**
   * Locate all files with the pending suffix under a directory.
   * @param pendingDir directory
   * @param recursive recursive listing?
   * @return iterator of all located entries
   * @throws IOException if there is a problem listing the path.
   */
  public RemoteIterator<LocatedFileStatus> locateAllSinglePendingCommits(
      Path pendingDir,
      boolean recursive) throws IOException {
    return listAndFilter(fs, pendingDir, recursive, PENDING_FILTER);
  }

  /**
   * Load all single pending commits in the directory, using the
   * outer submitter.
   * All load failures are logged and then added to list of files which would
   * not load.
   *
   * @param pendingDir directory containing commits
   * @param recursive do a recursive scan?
   * @param commitContext commit context
   *
   * @return tuple of loaded entries and those pending files which would
   * not load/validate.
   *
   * @throws IOException on a failure to list the files.
   */
  public Pair<PendingSet,
      List<Pair<LocatedFileStatus, IOException>>>
      loadSinglePendingCommits(Path pendingDir,
      boolean recursive,
      CommitContext commitContext)
      throws IOException {

    PendingSet commits = new PendingSet();
    List<SinglePendingCommit> pendingFiles = Collections.synchronizedList(
        new ArrayList<>(1));
    List<Pair<LocatedFileStatus, IOException>> failures = Collections.synchronizedList(
        new ArrayList<>(1));

    TaskPool.foreach(locateAllSinglePendingCommits(pendingDir, recursive))
        //. stopOnFailure()
        .suppressExceptions(false)
        .executeWith(commitContext.getOuterSubmitter())
        .run(status -> {
          Path path = status.getPath();
          try {
            // load the file
            SinglePendingCommit singleCommit = trackDuration(statistics,
                COMMITTER_LOAD_SINGLE_PENDING_FILE.getSymbol(), () ->
                    SinglePendingCommit.load(fs,
                        path,
                        status,
                        commitContext.getSinglePendingFileSerializer()));
            // aggregate stats
            commits.getIOStatistics()
                .aggregate(singleCommit.getIOStatistics());
            // then clear so they aren't marshalled again.
            singleCommit.getIOStatistics().clear();
            pendingFiles.add(singleCommit);
          } catch (IOException e) {
            LOG.warn("Failed to load commit file {}", path, e);
            failures.add(Pair.of(status, e));
          }
        });
    commits.setCommits(pendingFiles);
    return Pair.of(commits, failures);
  }

  /**
   * Convert any exception to an IOE, if needed.
   * @param key key to use in a path exception
   * @param ex exception
   * @return an IOE, either the passed in value or a new one wrapping the other
   * exception.
   */
  public IOException makeIOE(String key, Exception ex) {
    return ex instanceof IOException
           ? (IOException) ex
           : new PathCommitException(key, ex.toString(), ex);
  }

  /**
   * Abort the multipart commit supplied. This is the lower level operation
   * which doesn't generate an outcome, instead raising an exception.
   * @param commit pending commit to abort
   * @throws FileNotFoundException if the abort ID is unknown
   * @throws IOException on any failure
   */
  public void abortSingleCommit(SinglePendingCommit commit)
      throws IOException {
    String destKey = commit.getDestinationKey();
    String origin = commit.getFilename() != null
                    ? (" defined in " + commit.getFilename())
                    : "";
    String uploadId = commit.getUploadId();
    LOG.info("Aborting commit ID {} to object {}{}", uploadId, destKey, origin);
    abortMultipartCommit(destKey, uploadId);
  }

  /**
   * Create an {@code AbortMultipartUpload} request and POST it to S3,
   * incrementing statistics afterwards.
   * @param destKey destination key
   * @param uploadId upload to cancel
   * @throws FileNotFoundException if the abort ID is unknown
   * @throws IOException on any failure
   */
  public void abortMultipartCommit(String destKey, String uploadId)
      throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Aborting commit ID %s to path %s", uploadId, destKey)) {
      writeOperations.abortMultipartCommit(destKey, uploadId);
    } finally {
      statistics.commitAborted();
    }
  }

  /**
   * Enumerate all pending files in a dir/tree, abort.
   * @param pendingDir directory of pending operations
   * @param commitContext commit context
   * @param recursive recurse?
   * @return the outcome of all the abort operations
   * @throws IOException if there is a problem listing the path.
   */
  public MaybeIOE abortAllSinglePendingCommits(Path pendingDir,
      CommitContext commitContext,
      boolean recursive)
      throws IOException {
    Preconditions.checkArgument(pendingDir != null, "null pendingDir");
    LOG.debug("Aborting all pending commit filess under {}"
            + " (recursive={}", pendingDir, recursive);
    RemoteIterator<LocatedFileStatus> pendingFiles;
    try {
      pendingFiles = ls(pendingDir, recursive);
    } catch (FileNotFoundException fnfe) {
      LOG.info("No directory to abort {}", pendingDir);
      return MaybeIOE.NONE;
    }
    MaybeIOE outcome = MaybeIOE.NONE;
    if (!pendingFiles.hasNext()) {
      LOG.debug("No files to abort under {}", pendingDir);
    }
    while (pendingFiles.hasNext()) {
      final LocatedFileStatus status = pendingFiles.next();
      Path pendingFile = status.getPath();
      if (pendingFile.getName().endsWith(CommitConstants.PENDING_SUFFIX)) {
        try {
          abortSingleCommit(SinglePendingCommit.load(fs,
              pendingFile,
              status,
              commitContext.getSinglePendingFileSerializer()));
        } catch (FileNotFoundException e) {
          LOG.debug("listed file already deleted: {}", pendingFile);
        } catch (IOException | IllegalArgumentException e) {
          if (MaybeIOE.NONE.equals(outcome)) {
            outcome = new MaybeIOE(makeIOE(pendingFile.toString(), e));
          }
        } finally {
          // quietly try to delete the pending file
          S3AUtils.deleteQuietly(fs, pendingFile, false);
        }
      }
    }
    cleanupRemoteIterator(pendingFiles);
    return outcome;
  }

  /**
   * List files.
   * @param path path
   * @param recursive recursive listing?
   * @return iterator
   * @throws IOException failure
   */
  protected RemoteIterator<LocatedFileStatus> ls(Path path, boolean recursive)
      throws IOException {
    return fs.listFiles(path, recursive);
  }

  /**
   * List all pending uploads to the destination FS under a path.
   * @param dest destination path
   * @return A list of the pending uploads to any directory under that path.
   * @throws IOException IO failure
   */
  public List<MultipartUpload> listPendingUploadsUnderPath(Path dest)
      throws IOException {
    return writeOperations.listMultipartUploads(fs.pathToKey(dest));
  }

  /**
   * Abort all pending uploads to the destination FS under a path.
   * @param dest destination path
   * @return a count of the number of uploads aborted.
   * @throws IOException IO failure
   */
  public int abortPendingUploadsUnderPath(Path dest) throws IOException {
    return writeOperations.abortMultipartUploadsUnderPath(fs.pathToKey(dest));
  }

  /**
   * Delete any existing {@code _SUCCESS} file.
   * @param outputPath output directory
   * @throws IOException IO problem
   */
  public void deleteSuccessMarker(Path outputPath) throws IOException {
    fs.delete(new Path(outputPath, _SUCCESS), false);
  }

  /**
   * Save the success data to the {@code _SUCCESS} file.
   * @param outputPath output directory
   * @param successData success data to save.
   * @param addMetrics should the FS metrics be added?
   * @throws IOException IO problem
   */
  public void createSuccessMarker(Path outputPath,
      SuccessData successData,
      boolean addMetrics)
      throws IOException {
    Preconditions.checkArgument(outputPath != null, "null outputPath");

    if (addMetrics) {
      addFileSystemStatistics(successData.getMetrics());
    }

    // now write
    Path markerPath = new Path(outputPath, _SUCCESS);
    LOG.debug("Touching success marker for job {}: {}", markerPath,
        successData);
    try (DurationInfo ignored = new DurationInfo(LOG,
        "Writing success file %s", markerPath)) {
      successData.save(fs, markerPath, SuccessData.serializer());
    }
  }

  /**
   * Revert a pending commit by deleting the destination.
   * @param commit pending commit
   * @throws IOException failure
   */
  public void revertCommit(SinglePendingCommit commit) throws IOException {
    LOG.info("Revert {}", commit);
    try {
      writeOperations.revertCommit(commit.getDestinationKey());
    } finally {
      statistics.commitReverted();
    }
  }

  /**
   * Upload all the data in the local file, returning the information
   * needed to commit the work.
   * @param localFile local file (be  a file)
   * @param destPath destination path
   * @param partition partition/subdir. Not used
   * @param uploadPartSize size of upload
   * @param progress progress callback
   * @return a pending upload entry
   * @throws IOException failure
   */
  public SinglePendingCommit uploadFileToPendingCommit(File localFile,
      Path destPath,
      String partition,
      long uploadPartSize,
      Progressable progress)
      throws IOException {

    LOG.debug("Initiating multipart upload from {} to {}",
        localFile, destPath);
    Preconditions.checkArgument(destPath != null);
    if (!localFile.isFile()) {
      throw new FileNotFoundException("Not a file: " + localFile);
    }
    String destURI = destPath.toUri().toString();
    String destKey = fs.pathToKey(destPath);
    String uploadId = null;

    // flag to indicate to the finally clause that the operation
    // failed. it is cleared as the last action in the try block.
    boolean threw = true;
    final DurationTracker tracker = statistics.trackDuration(
        COMMITTER_STAGE_FILE_UPLOAD.getSymbol());
    try (DurationInfo d = new DurationInfo(LOG,
        "Upload staged file from %s to %s",
        localFile.getAbsolutePath(),
        destPath)) {

      statistics.commitCreated();
      uploadId = writeOperations.initiateMultiPartUpload(destKey,
          PutObjectOptions.keepingDirs());
      long length = localFile.length();

      SinglePendingCommit commitData = new SinglePendingCommit();
      commitData.setDestinationKey(destKey);
      commitData.setBucket(fs.getBucket());
      commitData.touch(System.currentTimeMillis());
      commitData.setUploadId(uploadId);
      commitData.setUri(destURI);
      commitData.setText(partition != null ? "partition: " + partition : "");
      commitData.setLength(length);

      long offset = 0;
      long numParts = (length / uploadPartSize +
          ((length % uploadPartSize) > 0 ? 1 : 0));
      // always write one part, even if it is just an empty one
      if (numParts == 0) {
        numParts = 1;
      }
      if (numParts > InternalConstants.DEFAULT_UPLOAD_PART_COUNT_LIMIT) {
        // fail if the file is too big.
        // it would be possible to be clever here and recalculate the part size,
        // but this is not currently done.
        throw new PathIOException(destPath.toString(),
            String.format("File to upload (size %d)"
                + " is too big to be uploaded in parts of size %d",
                numParts, length));
      }

      List<PartETag> parts = new ArrayList<>((int) numParts);

      LOG.debug("File size is {}, number of parts to upload = {}",
          length, numParts);
      for (int partNumber = 1; partNumber <= numParts; partNumber += 1) {
        progress.progress();
        long size = Math.min(length - offset, uploadPartSize);
        UploadPartRequest part;
        part = writeOperations.newUploadPartRequest(
            destKey,
            uploadId,
            partNumber,
            (int) size,
            null,
            localFile,
            offset);
        part.setLastPart(partNumber == numParts);
        UploadPartResult partResult = writeOperations.uploadPart(part, statistics);
        offset += uploadPartSize;
        parts.add(partResult.getPartETag());
      }

      commitData.bindCommitData(parts);
      statistics.commitUploaded(length);
      // clear the threw flag.
      threw = false;
      return commitData;
    } finally {
      if (threw && uploadId != null) {
        try {
          abortMultipartCommit(destKey, uploadId);
        } catch (IOException e) {
          LOG.error("Failed to abort upload {} to {}", uploadId, destKey, e);
        }
      }
      if (threw) {
        tracker.failed();
      }
      // close tracker and so report statistics of success/failure
      tracker.close();
    }
  }

  /**
   * Add the filesystem statistics to the map; overwriting anything
   * with the same name.
   * @param dest destination map
   */
  public void addFileSystemStatistics(Map<String, Long> dest) {
    dest.putAll(fs.getInstrumentation().toMap());
  }

  /**
   * Note that a task has completed.
   * @param success success flag
   */
  public void taskCompleted(boolean success) {
    statistics.taskCompleted(success);
  }

  /**
   * Note that a job has completed.
   * @param success success flag
   */
  public void jobCompleted(boolean success) {
    statistics.jobCompleted(success);
  }

  /**
   * Create a commit context for a job or task.
   *
   * @param context job context
   * @param path path for all work.
   * @param committerThreads thread pool size
   * @param ioStatisticsContext IOStatistics context of current thread
   * @return the commit context to pass in.
   * @throws IOException failure.
   */
  public CommitContext createCommitContext(
      JobContext context,
      Path path,
      int committerThreads,
      IOStatisticsContext ioStatisticsContext) throws IOException {
    return new CommitContext(this, context,
        committerThreads,
        ioStatisticsContext);
  }

  /**
   * Create a stub commit context for tests.
   * There's no job context.
   * @param path path for all work.
   * @param jobId job ID; if null a random UUID is generated.
   * @param committerThreads number of committer threads.
   * @return the commit context to pass in.
   * @throws IOException failure.
   */
  public CommitContext createCommitContextForTesting(
      Path path, @Nullable String jobId, int committerThreads) throws IOException {
    final String id = jobId != null
        ? jobId
        : UUID.randomUUID().toString();

    return new CommitContext(this,
        getStoreContext().getConfiguration(),
        id,
        committerThreads,
        IOStatisticsContext.getCurrentIOStatisticsContext());
  }

  /**
   * Get the magic file length of a file.
   * If the FS doesn't support the API, the attribute is missing or
   * the parse to long fails, then Optional.empty() is returned.
   * Static for some easier testability.
   * @param fs filesystem
   * @param path path
   * @return either a length or None.
   * @throws IOException on error
   * */
  public static Optional<Long> extractMagicFileLength(FileSystem fs, Path path)
      throws IOException {
    byte[] bytes;
    try {
      bytes = fs.getXAttr(path, XA_MAGIC_MARKER);
    } catch (UnsupportedOperationException e) {
      // FS doesn't support xattr.
      LOG.debug("Filesystem {} doesn't support XAttr API", fs);
      return Optional.empty();
    }
    return HeaderProcessing.extractXAttrLongValue(bytes);
  }

  /**
   * A holder for a possible IOException; the call {@link #maybeRethrow()}
   * will throw any exception passed into the constructor, and be a no-op
   * if none was.
   *
   * Why isn't a Java 8 optional used here? The main benefit would be that
   * {@link #maybeRethrow()} could be done as a map(), but because Java doesn't
   * allow checked exceptions in a map, the following code is invalid
   * <pre>
   *   exception.map((e) -&gt; {throw e;}
   * </pre>
   * As a result, the code to work with exceptions would be almost as convoluted
   * as the original.
   */
  public static class MaybeIOE {
    private final IOException exception;

    public static final MaybeIOE NONE = new MaybeIOE(null);

    /**
     * Construct with an exception.
     * @param exception exception
     */
    public MaybeIOE(IOException exception) {
      this.exception = exception;
    }

    /**
     * Get any exception.
     * @return the exception.
     */
    public IOException getException() {
      return exception;
    }

    /**
     * Is there an exception in this class?
     * @return true if there is an exception
     */
    public boolean hasException() {
      return exception != null;
    }

    /**
     * Rethrow any exception.
     * @throws IOException the exception field, if non-null.
     */
    public void maybeRethrow() throws IOException {
      if (exception != null) {
        throw exception;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("MaybeIOE{");
      sb.append(hasException() ? exception : "");
      sb.append('}');
      return sb.toString();
    }

    /**
     * Get an instance based on the exception: either a value
     * or a reference to {@link #NONE}.
     * @param ex exception
     * @return an instance.
     */
    public static MaybeIOE of(IOException ex) {
      return ex != null ? new MaybeIOE(ex) : NONE;
    }
  }

}
