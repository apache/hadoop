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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

import static java.util.Objects.requireNonNull;

/**
 * FileSystem operations which are needed to generate the task manifest.
 * The specific choice of which implementation to use is configurable.
 */
public abstract class StoreOperations implements Closeable {

  /**
   * Bind to the filesystem.
   * This is called by the manifest committer after the operations
   * have been instantiated.
   * @param fileSystem target FS
   * @param path actual path under FS.
   * @throws IOException if there are binding problems.
   */
  public void bindToFileSystem(FileSystem fileSystem, Path path) throws IOException {

  }

  /**
   * Forward to {@link FileSystem#getFileStatus(Path)}.
   * @param path path
   * @return status
   * @throws IOException failure.
   */
  public abstract FileStatus getFileStatus(Path path) throws IOException;

  /**
   * Is a path a file? Used during directory creation.
   * The is a copy & paste of FileSystem.isFile();
   * {@code StoreOperationsThroughFileSystem} calls into
   * the FS direct so that stores which optimize their probes
   * can save on IO.
   * @param path path to probe
   * @return true if the path exists and resolves to a file
   * @throws IOException failure other than FileNotFoundException
   */
  public boolean isFile(Path path) throws IOException {
    try {
      return getFileStatus(path).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Forward to {@link FileSystem#delete(Path, boolean)}.
   * If it returns without an error: there is nothing at
   * the end of the path.
   * @param path path
   * @param recursive recursive delete.
   * @return true if the path was deleted.
   * @throws IOException failure.
   */
  public abstract boolean delete(Path path, boolean recursive)
      throws IOException;

  /**
   * Forward to {@link FileSystem#mkdirs(Path)}.
   * Usual "what does 'false' mean" ambiguity.
   * @param path path
   * @return true if the directory was created.
   * @throws IOException failure.
   */
  public abstract boolean mkdirs(Path path) throws IOException;

  /**
   * Forward to {@link FileSystem#rename(Path, Path)}.
   * Usual "what does 'false' mean" ambiguity.
   * @param source source file
   * @param dest destination path -which must not exist.
   * @return true if the directory was created.
   * @throws IOException failure.
   */
  public abstract boolean renameFile(Path source, Path dest)
      throws IOException;

  /**
   * Rename a dir; defaults to invoking
   * Forward to {@link #renameFile(Path, Path)}.
   * Usual "what does 'false' mean" ambiguity.
   * @param source source file
   * @param dest destination path -which must not exist.
   * @return true if the directory was created.
   * @throws IOException failure.
   */
  public boolean renameDir(Path source, Path dest)
      throws IOException {
    return renameFile(source, dest);
  }

  /**
   * List the directory.
   * @param path path to list.
   * @return an iterator over the results.
   * @throws IOException any immediate failure.
   */
  public abstract RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException;

  /**
   * Load a task manifest from the store.
   * with a real FS, this is done with
   * {@link TaskManifest#load(JsonSerialization, FileSystem, Path, FileStatus)}
   *
   * @param serializer serializer.
   * @param st status with the path and other data.
   * @return the manifest
   * @throws IOException failure to load/parse
   */
  public abstract TaskManifest loadTaskManifest(
      JsonSerialization<TaskManifest> serializer,
      FileStatus st) throws IOException;

  /**
   * Save a task manifest by create(); there's no attempt at
   * renaming anything here.
   * @param manifestData the manifest/success file
   * @param path temp path for the initial save
   * @param overwrite should create(overwrite=true) be used?
   * @throws IOException failure to load/parse
   */
  public abstract <T extends AbstractManifestData<T>> void save(
      T manifestData,
      Path path,
      boolean overwrite) throws IOException;

  /**
   * Is trash enabled for the given store.
   * @param path path to move, assumed to be _temporary
   * @return true iff trash is enabled.
   */
  public abstract boolean isTrashEnabled(Path path);

  /**
   * Make an msync() call; swallow when unsupported.
   * @param path path
   * @throws IOException IO failure
   */
  public abstract void msync(Path path) throws IOException;

  /**
   * Extract an etag from a status if the conditions are met.
   * If the conditions are not met, return null or ""; they will
   * both be treated as "no etags available"
   * <pre>
   *   1. The status is of a type which the implementation recognizes
   *   as containing an etag.
   *   2. After casting the etag field can be retrieved
   *   3. and that value is non-null/non-empty.
   * </pre>
   * @param status status, which may be null of any subclass of FileStatus.
   * @return either a valid etag, or null or "".
   */
  public String getEtag(FileStatus status) {
    return ManifestCommitterSupport.getEtag(status);
  }

  /**
   * Does the store preserve etags through renames.
   * If true, and if the source listing entry has an etag,
   * it will be used to attempt to validate a failed rename.
   * @param path path to probe.
   * @return true if etag comparison is a valid strategy.
   */
  public boolean storePreservesEtagsThroughRenames(Path path) {
    return false;
  }

  /**
   * Does the store provide rename resilience through etags?
   * If true then {@link #commitFile(FileOrDirEntry)} will
   * have any supplied etags used to recover from rename
   * failures.
   * @return true if etag comparison is used for recovery.
   */
  public boolean storeSupportsResilientCommitThroughEtags() {
    return false;
  }

  /**
   * Commit one file.
   * The uses {@link #renameFile(Path, Path)}.
   * If etags were collected during task commit, these will be
   * in the entries passed in here.
   * @param entry entry to commit
   * @return the result of rename()
   * @throws IOException failure.
   */
  public CommitFileResult commitFile(FileOrDirEntry entry) throws IOException {
    return CommitFileResult.fromRename(renameFile(entry.getSourcePath(), entry.getDestPath()));
  }

  /**
   * Outcome from the commit.
   */
  public static final class CommitFileResult {

    /** result of any rename() call. */
    private final boolean renameOutcome;

    /** Did recovery take place? */
    private final boolean recovered;

    /**
     * Create from a classic rename.
     * @param renameOutcome the outcome
     */
    public static CommitFileResult fromRename(final boolean renameOutcome) {
        return new CommitFileResult(false, renameOutcome);
    }

    /**
     * Full commit result.
     * @param recovered Did recovery take place?
     */
    public static CommitFileResult fromResilientCommit(final boolean recovered) {
        return new CommitFileResult(recovered, true);
    }

    /**
     * Full commit result.
     * @param recovered Did recovery take place?
     * @param renameOutcome result of any rename() call.
     */
    private CommitFileResult(
        final boolean recovered,
        final boolean renameOutcome) {
      this.renameOutcome = renameOutcome;
      this.recovered = recovered;
    }

    public boolean isRenameOutcome() {
      return renameOutcome;
    }

    public boolean isRecovered() {
      return recovered;
    }

    /**
     * Successful?
     * Implicit in a commit via the resilient API; for classic rename it's
     * the boolean return value.
     * @return true if the operation successfully moved the file.
     */
    public boolean success() {
      return recovered || renameOutcome;
    }
  }

  /**
   * Move a directory to trash, with the jobID as its name.
   * IOExceptions are caught and included in the outcome.
   * @param jobId job ID.
   * @param path path to move, assumed to be _temporary
   * @return the outcome.
   */
  public abstract MoveToTrashResult moveToTrash(String jobId, Path path);

  /**
   * Enum of outcomes.
   */
  public enum MoveToTrashOutcome {
    DISABLED("Disabled"),
    RENAMED_TO_TRASH("Renamed under trash"),
    FAILURE("Rename failed");

    private final String description;

    MoveToTrashOutcome(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return "Outcome{" + name() +
          " '" + description + '\'' +
          "}";
    }
  }

  /**
   * Result.
   * if renamed == false then failureIOE must be non-null.
   */
  public static final class MoveToTrashResult {

    private final MoveToTrashOutcome outcome;
    private final IOException exception;

    public MoveToTrashResult(MoveToTrashOutcome outcome,
        IOException exception) {
      if (outcome != MoveToTrashOutcome.RENAMED_TO_TRASH) {
        requireNonNull(exception, "No exception in failure result");
      }
      this.outcome = outcome;
      this.exception = exception;
    }

    public MoveToTrashOutcome getOutcome() {
      return outcome;
    }

    public boolean wasRenamed() {
      return outcome == MoveToTrashOutcome.RENAMED_TO_TRASH;
    }

    public IOException getException() {
      return exception;
    }

    @Override
    public String toString() {
      return "MoveToTrashResult{" +
          "renamed=" + outcome +
          ", failure=" + exception +
          '}';
    }
  }
}
