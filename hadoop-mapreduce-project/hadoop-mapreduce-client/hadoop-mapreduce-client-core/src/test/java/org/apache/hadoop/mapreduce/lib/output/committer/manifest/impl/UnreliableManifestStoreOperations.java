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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.JsonSerialization;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.OPERATION_TIMED_OUT;

/**
 * Wrap an existing {@link ManifestStoreOperations} implementation and fail on
 * specific paths.
 * This is for testing. It could be implemented via
 * Mockito 2 spy code but is not so that:
 * 1. It can be backported to Hadoop versions using Mockito 1.x.
 * 2. It can be extended to use in production. This is why it is in
 * the production module -to allow for downstream tests to adopt it.
 * 3. You can actually debug what's going on.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UnreliableManifestStoreOperations extends ManifestStoreOperations {

  private static final Logger LOG = LoggerFactory.getLogger(
      UnreliableManifestStoreOperations.class);

  /**
   * The timeout message ABFS raises.
   */
  public static final String E_TIMEOUT
      = "Operation could not be completed within the specified time";

  /**
   * Text to use in simulated failure exceptions.
   */
  public static final String SIMULATED_FAILURE = "Simulated failure";

  /**
   * Underlying store operations to wrap.
   */
  private final ManifestStoreOperations wrappedOperations;

  /**
   * Paths of delete operations to fail.
   */
  private final Set<Path> deletePathsToFail = new HashSet<>();

  /**
   * Paths of delete operations to time out, as ABFS may.
   */
  private final Set<Path> deletePathsToTimeOut = new HashSet<>();

  /**
   * Paths of List operations to fail.
   */
  private final Set<Path> listToFail = new HashSet<>();

  /**
   * Paths of mkdirs operations to fail.
   */
  private final Set<Path> mkdirsToFail = new HashSet<>();

  /**
   * Paths which don't exist.
   */
  private final Set<Path> pathNotFound = new HashSet<>();

  /**
   * Source file whose rename/commit will fail.
   */
  private final Set<Path> renameSourceFilesToFail = new HashSet<>();

  /**
   * Dest dir into which all renames/commits will fail.
   * Subdirectories under this are not checked.
   */
  private final Set<Path> renameDestDirsToFail = new HashSet<>();

  /**
   * Path of save() to fail.
   */
  private final Set<Path> saveToFail = new HashSet<>();

  /**
   * timeout sleep.
   */
  private int timeoutSleepTimeMillis;

  /**
   * Should rename thrown an exception or just return false.
   */
  private boolean renameToFailWithException = true;

  /**
   * Constructor.
   * @param wrappedOperations operations to wrap.
   */
  public UnreliableManifestStoreOperations(final ManifestStoreOperations wrappedOperations) {
    this.wrappedOperations = wrappedOperations;
  }


  /**
   * Reset everything.
   */
  public void reset() {
    deletePathsToFail.clear();
    deletePathsToTimeOut.clear();
    pathNotFound.clear();
    renameSourceFilesToFail.clear();
    renameDestDirsToFail.clear();
    timeoutSleepTimeMillis = 0;
  }

  public int getTimeoutSleepTimeMillis() {
    return timeoutSleepTimeMillis;
  }

  public void setTimeoutSleepTimeMillis(final int timeoutSleepTimeMillis) {
    this.timeoutSleepTimeMillis = timeoutSleepTimeMillis;
  }

  public boolean getRenameToFailWithException() {
    return renameToFailWithException;
  }

  public void setRenameToFailWithException(
      final boolean renameToFailWithException) {
    this.renameToFailWithException = renameToFailWithException;
  }

  /**
   * Add a path to the list of delete paths to fail.
   * @param path path to add.
   */
  public void addDeletePathToFail(Path path) {
    deletePathsToFail.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of delete paths to time out.
   * @param path path to add.
   */
  public void addDeletePathToTimeOut(Path path) {
    deletePathsToTimeOut.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of paths where list will fail.
   * @param path path to add.
   */
  public void addListToFail(Path path) {
    listToFail.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of mkdir calls to fail.
   * @param path path to add.
   */
  public void addMkdirsToFail(Path path) {
    mkdirsToFail.add(requireNonNull(path));
  }

  /**
   * Add a path not found.
   * @param path path
   */
  public void addPathNotFound(Path path) {
    pathNotFound.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of rename source paths to fail.
   * @param path path to add.
   */
  public void addRenameSourceFilesToFail(Path path) {
    renameSourceFilesToFail.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of dest dirs to fail.
   * @param path path to add.
   */
  public void addRenameDestDirsFail(Path path) {
    renameDestDirsToFail.add(requireNonNull(path));
  }

  /**
   * Add a path to the list of paths where save will fail.
   * @param path path to add.
   */
  public void addSaveToFail(Path path) {
    saveToFail.add(requireNonNull(path));
  }

  /**
   * Raise an exception if the path is in the set of target paths.
   * @param operation operation which failed.
   * @param path path to check
   * @param paths paths to probe for {@code path} being in.
   * @throws IOException simulated failure
   */
  private void maybeRaiseIOE(String operation, Path path, Set<Path> paths)
      throws IOException {
    if (paths.contains(path)) {
      LOG.info("Simulating failure of {} with {}", operation, path);
      throw new PathIOException(path.toString(),
          SIMULATED_FAILURE + " of " + operation);
    }
  }

  /**
   * Verify that a path is not on the file not found list.
   * @param path path
   * @throws FileNotFoundException if configured to fail.
   */
  private void verifyExists(Path path) throws FileNotFoundException {
    if (pathNotFound.contains(path)) {
      throw new FileNotFoundException(path.toString());
    }
  }

  /**
   * Time out if the path is in the list of timeout paths.
   * Will sleep first, to help simulate delays.
   * @param operation operation which failed.
   * @param path path to check
   * @param paths paths to probe for {@code path} being in.
   * @throws IOException simulated timeout
   */
  private void maybeTimeout(String operation, Path path, Set<Path> paths)
      throws IOException {
    if (paths.contains(path)) {
      LOG.info("Simulating timeout of {} with {}", operation, path);
      try {
        if (timeoutSleepTimeMillis > 0) {
          Thread.sleep(timeoutSleepTimeMillis);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.toString());
      }
      throw new PathIOException(path.toString(),
          "ErrorCode=" + OPERATION_TIMED_OUT
              + " ErrorMessage=" + E_TIMEOUT);
    }
  }

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    verifyExists(path);
    return wrappedOperations.getFileStatus(path);
  }

  @Override
  public boolean delete(final Path path, final boolean recursive)
      throws IOException {
    String op = "delete";
    maybeTimeout(op, path, deletePathsToTimeOut);
    maybeRaiseIOE(op, path, deletePathsToFail);
    return wrappedOperations.delete(path, recursive);
  }

  @Override
  public boolean mkdirs(final Path path) throws IOException {
    maybeRaiseIOE("mkdirs", path, mkdirsToFail);
    return wrappedOperations.mkdirs(path);
  }

  @Override
  public boolean renameFile(final Path source, final Path dest)
      throws IOException {
    String op = "rename";
    if (renameToFailWithException) {
      maybeRaiseIOE(op, source, renameSourceFilesToFail);
      maybeRaiseIOE(op, dest.getParent(), renameDestDirsToFail);
    } else {
      if (renameSourceFilesToFail.contains(source)
          || renameDestDirsToFail.contains(dest.getParent())) {
        LOG.info("Failing rename({}, {})", source, dest);
        return false;
      }
    }
    return wrappedOperations.renameFile(source, dest);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path path)
      throws IOException {
    verifyExists(path);
    maybeRaiseIOE("listStatus", path, listToFail);
    return wrappedOperations.listStatusIterator(path);
  }

  @Override
  public TaskManifest loadTaskManifest(JsonSerialization<TaskManifest> serializer,
      final FileStatus st) throws IOException {
    verifyExists(st.getPath());
    return wrappedOperations.loadTaskManifest(serializer, st);
  }

  @Override
  public <T extends AbstractManifestData<T>> void save(T manifestData,
      final Path path,
      final boolean overwrite) throws IOException {
    maybeRaiseIOE("save", path, saveToFail);
    wrappedOperations.save(manifestData, path, overwrite);
  }

  @Override
  public void msync(Path path) throws IOException {
    wrappedOperations.msync(path);
  }

  @Override
  public String getEtag(FileStatus status) {
    return wrappedOperations.getEtag(status);
  }

  @Override
  public boolean storeSupportsResilientCommit() {
    return wrappedOperations.storeSupportsResilientCommit();
  }

  @Override
  public CommitFileResult commitFile(final FileEntry entry)
      throws IOException {
    if (renameToFailWithException) {
      maybeRaiseIOE("commitFile",
          entry.getSourcePath(), renameSourceFilesToFail);
      maybeRaiseIOE("commitFile",
          entry.getDestPath().getParent(), renameDestDirsToFail);
    }
    return wrappedOperations.commitFile(entry);
  }

  @Override
  public boolean storePreservesEtagsThroughRenames(Path path) {
    return wrappedOperations.storePreservesEtagsThroughRenames(path);
  }

  @Override
  public void close() throws IOException {
    wrappedOperations.close();
  }

}
