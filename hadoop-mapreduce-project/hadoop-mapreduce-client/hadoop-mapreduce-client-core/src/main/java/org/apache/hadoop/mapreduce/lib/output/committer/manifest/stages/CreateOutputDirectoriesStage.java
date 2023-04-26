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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.measureDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MKDIRS_RETURNED_FALSE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_PREPARE_DIR_ANCESTORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.util.OperationDuration.humanTime;

/**
 * Prepare the destination directory tree, as efficiently as possible.
 * possible -and doing those IO operations in the thread pool.
 *
 * The classic FileOutputCommitter does a recursive treewalk and
 * deletes any files found at paths where directories are to be created.
 *
 * Each task manifest's directories are combined with those of the other tasks
 * to build a set of all directories which are needed, without duplicates.
 *
 * This stage requires the aggregate set of manifests to contain
 * all directories to create, including level,
 * and expects them to have been probed for existence/state.
 *
 * For each level, all dirs are processed in parallel to
 * be created or, if files, deleted.
 *
 * The stage returns the list of directories created, and for testing,
 * the map of paths to outcomes.
 *
 * Directory creation can be surprisingly slow against object stores,
 * do use benchmarks from real test runs when tuning this algorithm.
 */
public class CreateOutputDirectoriesStage extends
    AbstractJobOrTaskStage<
        Collection<DirEntry>,
        CreateOutputDirectoriesStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CreateOutputDirectoriesStage.class);

  /**
   * Directories as a map of (path, DirMapState).
   */
  private final Map<Path, DirMapState> dirMap = new ConcurrentHashMap<>();

  /**
   * A list of created paths for the results.
   */
  private final List<Path> createdDirectories = new ArrayList<>();

  public CreateOutputDirectoriesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CREATE_TARGET_DIRS, true);
    // add the dest dir to the dir map as we expect the job setup to create it.
    dirMap.put(getDestinationDir(), DirMapState.dirWasCreated);
  }

  @Override
  protected Result executeStage(
      final Collection<DirEntry> manifestDirs)
      throws IOException {

    final List<Path> directories = createAllDirectories(manifestDirs);
    LOG.debug("{}: Created {} directories", getName(), directories.size());
    return new Result(new HashSet<>(directories), dirMap);
  }

  /**
   * Build the list of directories to create.
   * @param manifestDirs dir entries from the manifests
   * @return the list of paths which have been created.
   */
  private List<Path> createAllDirectories(final Collection<DirEntry> manifestDirs)
      throws IOException {

    // all directories which need to exist across all
    // tasks.
    // leaf directories
    final Map<Path, DirEntry> leaves = new HashMap<>();
    // parent directories. these do not need to be
    // explicitly created.
    final Map<Path, DirEntry> parents = new HashMap<>();
    // the files which must be deleted as a directory
    // will be created at that path.
    final Set<Path> filesToDelete = new HashSet<>();

    // sort the values of dir map by directory level: parent dirs will
    // come first in the sorting
    List<DirEntry> destDirectories = new ArrayList<>(manifestDirs);

    Collections.sort(destDirectories, Comparator.comparingInt(DirEntry::getLevel));
    // iterate through the directory map
    for (DirEntry entry: destDirectories) {
      // add the dest entry
      final Path path = entry.getDestPath();
      if (!leaves.containsKey(path)) {
        leaves.put(path, entry);

        // if it is a file to delete, record this.
        if (entry.getStatus() == EntryStatus.file) {
          filesToDelete.add(path);
        }
        final Path parent = path.getParent();
        if (parent != null && leaves.containsKey(parent)) {
          // there's a parent dir, move it from the leaf list
          // to parent list
          parents.put(parent, leaves.remove(parent));
        }
      }
    }

    // at this point then there is a map of all directories which
    // are leaf entries and so need to be created if not present,
    // and the maximum level is known.
    // we can iterate through all levels deleting any files if there are any.

    // Prepare parent directories.
    deleteFiles(filesToDelete);

    // Now the real work.
    final int createCount = leaves.size();
    LOG.info("Preparing {} directory/directories; {} parent dirs implicitly created",
        createCount, parents.size());

    // now probe for and create the leaf dirs, which are those at the
    // bottom level
    Duration d = measureDurationOfInvocation(getIOStatistics(), OP_CREATE_DIRECTORIES, () ->
        TaskPool.foreach(leaves.values())
            .executeWith(getIOProcessors(createCount))
            .onFailure(this::reportMkDirFailure)
            .stopOnFailure()
            .run(this::createOneDirectory));
    LOG.info("Time to prepare directories {}", humanTime(d.toMillis()));
    return createdDirectories;
  }

  /**
   * How many failures have been reported.
   */
  private final AtomicInteger failureCount = new AtomicInteger();

  /**
   * report a single directory failure.
   * @param dirEntry dir which could not be deleted
   * @param e exception raised.
   */
  private void reportMkDirFailure(DirEntry dirEntry, Exception e) {
    Path path = dirEntry.getDestPath();
    final int count = failureCount.incrementAndGet();
    LOG.warn("{}: mkdir failure #{} Failed to create directory \"{}\": {}",
        getName(), count, path, e.toString());
    LOG.debug("{}: Full exception details",
        getName(), e);
  }

  /**
   * Delete all directories where there is a file.
   * @param filesToDelete set of dirs to where there is a file.
   * @throws IOException IO problem
   */
  private void deleteFiles(final Set<Path> filesToDelete)
      throws IOException {

    final int size = filesToDelete.size();
    if (size == 0) {
      // nothing to delete.
      return;
    }
    LOG.info("{}: Directory entries containing files to delete: {}", getName(), size);
    Duration d = measureDurationOfInvocation(getIOStatistics(),
        OP_PREPARE_DIR_ANCESTORS, () ->
            TaskPool.foreach(filesToDelete)
                .executeWith(getIOProcessors(size))
                .stopOnFailure()
                .run(dir -> {
                  updateAuditContext(OP_PREPARE_DIR_ANCESTORS);
                  deleteDirWithFile(dir);
                }));
    LOG.info("Time to delete files {}", humanTime(d.toMillis()));
  }

  /**
   * Prepare a parent directory.
   * @param dir directory to probe
   * @throws IOException failure in probe other than FNFE
   */
  private void deleteDirWithFile(Path dir) throws IOException {
    // report progress back
    progress();
    LOG.info("{}: Deleting file {}", getName(), dir);
    delete(dir, false, OP_DELETE);
    // note its final state
    addToDirectoryMap(dir, DirMapState.fileNowDeleted);
  }


  /**
   * Create a directory is required, updating the directory map
   * and, if the operation took place, the list of created dirs.
   * Reports progress on invocation.
   * @param dirEntry entry
   * @throws PathIOException if after multiple attempts, the dest dir couldn't be created.
   * @throws IOException failure.
   */
  private void createOneDirectory(final DirEntry dirEntry) throws IOException {
    // report progress back
    progress();
    final Path dir = dirEntry.getDestPath();
    updateAuditContext(OP_STAGE_JOB_CREATE_TARGET_DIRS);
    final DirMapState state = maybeCreateOneDirectory(dirEntry);
    switch (state) {
    case dirFoundInStore:
      addToDirectoryMap(dir, state);
      break;
    case dirWasCreated:
    case dirCreatedOnSecondAttempt:
      addCreatedDirectory(dir);
      addToDirectoryMap(dir, state);
      break;
    default:
      break;
    }

  }


  /**
   * Try to efficiently and robustly create a directory in a method which is
   * expected to be executed in parallel with operations creating
   * peer directories.
   * A return value of {@link DirMapState#dirWasCreated} or
   * {@link DirMapState#dirCreatedOnSecondAttempt} indicates
   * this thread did the creation.
   * Other outcomes imply it already existed; if the directory
   * cannot be created/found then a {@link PathIOException} is thrown.
   * The outcome should be added to the {@link #dirMap} to avoid further creation attempts.
   * @param dirEntry dir to create
   * @return Outcome of the operation, such as whether the entry was created, found in store.
   *           It will always be a success outcome of some form.
   * @throws PathIOException if after multiple attempts, the dest dir couldn't be created.
   * @throws IOException Other IO failure
   */
  private DirMapState maybeCreateOneDirectory(DirEntry dirEntry) throws IOException {
    final EntryStatus status = dirEntry.getStatus();
    if (status == EntryStatus.dir) {
      return DirMapState.dirFoundInStore;
    }
    // present in case directories are ever created in task commits
    if (status == EntryStatus.created_dir) {
      return DirMapState.dirWasCreated;
    }

    // here the dir doesn't exist because
    // it was a file and has been deleted, or
    // checks failed. create it.
    final Path path = dirEntry.getDestPath();

    LOG.info("Creating directory {}", path);

    try {
      if (mkdirs(path, false)) {
        // success -return immediately.
        return DirMapState.dirWasCreated;
      }
      getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);

      LOG.info("{}: mkdirs({}) returned false, attempting to recover",
          getName(), path);
    } catch (IOException e) {
      // can be caused by file existing, etc.
      LOG.info("{}: mkdir({}) raised exception {}", getName(), path, e.toString());
      LOG.debug("{}: Mkdir stack", getName(), e);
    }

    // fallback to checking the FS, in case a different process did it.
    FileStatus st = getFileStatusOrNull(path);
    if (st != null) {
      if (!st.isDirectory()) {
        // is bad: delete a file
        LOG.info("{}: Deleting file where a directory should go: {}",
            getName(), st);
        delete(path, false, OP_DELETE_FILE_UNDER_DESTINATION);
      } else {
        // is good.
        LOG.warn("{}: Even though mkdirs({}) failed, there is now a directory there",
            getName(), path);
        return DirMapState.dirFoundInStore;
      }
    } else {
      // nothing found. This should never happen.
      LOG.warn("{}: Although mkdirs({}) returned false, there's nothing at that path to prevent it",
          getName(), path);

    }

    // try to create the directory again
    // if this fails, and IOE is still raised, that
    // propagate to the caller.
    if (!mkdirs(path, false)) {

      // mkdirs failed again
      getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);

      // require the dir to exist, raising an exception if it does not.
      directoryMustExist("Creating directory ", path);
    }

    // we only get here if the second attempt recovered
    return DirMapState.dirCreatedOnSecondAttempt;

  }

  /**
   * Add a created dir to the list of created dirs.
   * @param dir new dir.
   */
  private void addCreatedDirectory(final Path dir) {
    synchronized (createdDirectories) {
      createdDirectories.add(dir);
    }
  }

  /**
   * Add a dir  to the directory map if there is not already an entry there.
   * @param dir directory.
   * @param state state of entry
   */
  private void addToDirectoryMap(final Path dir,
      DirMapState state) {
    if (!dirMap.containsKey(dir)) {
      dirMap.put(dir, state);
    }
  }


  /**
   * Result of the operation.
   */
  public static final class Result {

    /** directories created. */
    private final Set<Path> createdDirectories;

    /**
     * Map of dirs built up during preparation.
     */
    private final Map<Path, DirMapState> dirMap;

    public Result(Set<Path> createdDirectories,
        Map<Path, DirMapState> dirMap) {
      this.createdDirectories = requireNonNull(createdDirectories);
      this.dirMap = requireNonNull(dirMap);
    }

    public Set<Path> getCreatedDirectories() {
      return createdDirectories;
    }

    public Map<Path, DirMapState> getDirMap() {
      return dirMap;
    }

    @Override
    public String toString() {
      return "Result{" +
          "directory count=" + createdDirectories.size() +
          '}';
    }
  }

  /**
   * Enumeration of dir states in the dir map.
   */
  public enum DirMapState {
    dirFoundInStore,
    dirFoundInMap,
    dirWasCreated,
    dirCreatedOnSecondAttempt,
    fileNowDeleted,
    ancestorWasDirOrMissing,
    parentWasNotFile,
    parentOfCreatedDir
  }

}
