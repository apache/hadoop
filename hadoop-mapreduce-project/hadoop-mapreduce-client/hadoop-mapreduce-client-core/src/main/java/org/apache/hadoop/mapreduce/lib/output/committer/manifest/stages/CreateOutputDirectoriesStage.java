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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.measureDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_PREPARE_PARENT_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MKDIRS_RETURNED_FALSE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_PREPARE_DIR_ANCESTORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.util.OperationDuration.humanTime;

/**
 * Prepare the destination directory tree, by making as few IO calls as
 * possible -and doing those IO operations in the thread pool.
 *
 * The classic FileOutputCommitter does a recursive treewalk and
 * deletes any files found at paths where directories are to be created.
 *
 *
 * Each task manifest's directories are combined with those of the other tasks
 * to build a set of all directories which are needed, without duplicates.
 *
 * If the option to prepare parent directories was set,
 * these are also prepared for the commit.
 * A set of ancestor directories is built up, all of which MUST NOT
 * be files and SHALL be in one of two states
 * - directory
 * - nonexistent
 * Across a thread pool, An isFile() probe is made for all parents;
 * if there is a file there then it is deleted.
 *
 * After any ancestor dir preparation, the final dir set is created
 * though mkdirs() calls, with some handling for race conditions.
 * It is not an error if mkdirs() fails because the dest dir is there;
 * if it fails because a file was found -that file will be deleted.
 *
 * The stage returns the list of directories created, and for testing,
 * the map of paths to outcomes.
 */
public class CreateOutputDirectoriesStage extends
    AbstractJobCommitStage<List<TaskManifest>, CreateOutputDirectoriesStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CreateOutputDirectoriesStage.class);

  /**
   * Message to print on first mkdir failure if the parent dirs were not
   * prepared.
   */
  private static final String HINT_PREPARE_PARENT_DIR = "Setting "
      + OPT_PREPARE_PARENT_DIRECTORIES + " to \"true\" performs more parent directory"
      + " preparation and so may fix this problem";

  /**
   * Directories as a map of (path, path).
   * Using a map rather than any set for efficient concurrency; the
   * concurrent sets don't do lookups so fast.
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
      final List<TaskManifest> taskManifests)
      throws IOException {

    final List<Path> directories = createAllDirectories(taskManifests);
    LOG.debug("{}: Created {} directories", getName(), directories.size());
    return new Result(directories, dirMap);
  }

  /**
   * For each task, build the list of directories it wants.
   * @param taskManifests task manifests
   * @return the list of paths which have been created.
   */
  private List<Path> createAllDirectories(final List<TaskManifest> taskManifests)
      throws IOException {

    // the set is all directories which need to exist across all
    // tasks.
    // This set is probed during parent dir scanning, so
    //  needs to be efficient to look up values.
    final Set<Path> directoriesToCreate = new HashSet<>();

    // iterate through the task manifests
    // and all output dirs into the set of dirs to
    // create.
    // hopefully there is a lot of overlap, so the
    // final number of dirs to create is small.
    for (TaskManifest task : taskManifests) {
      final List<FileOrDirEntry> dirEntries
          = task.getDirectoriesToCreate();
      for (FileOrDirEntry entry: dirEntries) {
        // add the dest path
        directoriesToCreate.add(entry.getDestPath());
      }
    }

    // Prepare parent directories.
    if (getStageConfig().getPrepareParentDirectories()) {
      prepareParentDirectories(directoriesToCreate);
    }

    // Now the real work.

    final int createCount = directoriesToCreate.size();
    LOG.info("Preparing {} directory/directories", createCount);
    // now probe for and create the parent dirs of all renames
    Duration d = measureDurationOfInvocation(getIOStatistics(), OP_CREATE_DIRECTORIES, () ->
        TaskPool.foreach(directoriesToCreate)
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
   * @param path path which could not be deleted
   * @param e exception raised.
   */
  private void reportMkDirFailure(Path path, Exception e) {
    final int count = failureCount.incrementAndGet();
    LOG.warn("{}: mkdir failure #{} Failed to create directory \"{}\": {}",
        getName(), count, path, e.toString());
    LOG.debug("{}: Full exception details",
        getName(), e);
    if (count == 1  && !getStageConfig().getPrepareParentDirectories()) {
      // first failure and no parent dir setup, so recommend
      // preparing the parent dir
      LOG.warn(HINT_PREPARE_PARENT_DIR);
    }
  }

  /**
   * Clean up parent dirs. No attempt is made to create them, because
   * we know mkdirs() will do that (assuming it has the permissions,
   * of course)
   * @param directoriesToCreate set of dirs to create
   * @throws IOException IO problem
   */
  private void prepareParentDirectories(final Set<Path> directoriesToCreate)
      throws IOException {

    // include parents in the paths
    final Set<Path> ancestors = extractAncestors(
        getStageConfig().getDestinationDir(),
        directoriesToCreate);

    final int ancestorCount = ancestors.size();
    LOG.info("Preparing {} ancestor directory/directories", ancestorCount);
    // if there is a single ancestor, don't bother with parallisation.
    Duration d = measureDurationOfInvocation(getIOStatistics(),
        OP_PREPARE_DIR_ANCESTORS, () ->
            TaskPool.foreach(ancestors)
                .executeWith(getIOProcessors(ancestorCount))
                .stopOnFailure()
                .run(dir -> {
                  updateAuditContext(OP_PREPARE_DIR_ANCESTORS);
                  prepareParentDir(dir);
                }));
    LOG.info("Time to prepare ancestor directories {}", humanTime(d.toMillis()));
  }

  /**
   * Prepare a parent directory.
   * @param dir directory to probe
   * @throws IOException failure in probe other than FNFE
   */
  private void prepareParentDir(Path dir) throws IOException {
    // report progress back
    progress();
    LOG.debug("{}: Probing parent dir {}", getName(), dir);
    if (isFile(dir)) {
      // it's a file: delete it.
      LOG.info("{}: Deleting file {}", getName(), dir);
      delete(dir, false, OP_DELETE_FILE_UNDER_DESTINATION);
      // note its final state
      addToDirectoryMap(dir, DirMapState.ancestorWasFileNowDeleted);
    } else {
      // and add to dir map as a dir or missing entry
      LOG.debug("{}: Dir {} is missing or a directory", getName(), dir);
      addToDirectoryMap(dir, DirMapState.ancestorWasDirOrMissing);
    }
  }

  /**
   * Determine all ancestors of the dirs to create which are
   * under the dest path, not in the set of dirs to create.
   * Static and public for testability.
   * @param destDir destination dir of the job
   * @param directoriesToCreate set of dirs to create from tasks.
   * @return the set of parent dirs
   */
  public static Set<Path> extractAncestors(
      final Path destDir,
      final Collection<Path> directoriesToCreate) {

    final Set<Path> ancestors = new HashSet<>(directoriesToCreate.size());
    // minor optimization to save a set lookup on the common case where
    // multiple dirs in the list contain the same parent
    Path previousAncestor = destDir;
    for (Path dir : directoriesToCreate) {
      if (dir.isRoot()) {
        // sanity check
        LOG.warn("Root directory {} is one of the destination directories", dir);
        break;
      }
      if (ancestors.contains(dir) || destDir.equals(dir)) {
        // dir is in the ancestor set or is the destDir itself
        continue;
      }
      // get its ancestor
      Path ancestor = dir.getParent();
      // add all the parents.
      while (!ancestor.isRoot()
          && !ancestor.equals(destDir)
          && !ancestor.equals(previousAncestor)
          && !ancestors.contains(ancestor)
          && !directoriesToCreate.contains(ancestor)) {
        // add to the set of dirs to create
        ancestors.add(ancestor);
        // and determine next parent
        ancestor = ancestor.getParent();
      }
      // and remember the previous, so that we can bail out fast if there is a
      // set of partitions.
      previousAncestor = dir.getParent();
    }
    return ancestors;
  }

  private void createOneDirectory(final Path dir) throws IOException {
    // report progress back
    progress();
    updateAuditContext(OP_STAGE_JOB_CREATE_TARGET_DIRS);
    final DirMapState state = maybeCreateOneDirectory(dir);
    switch (state) {
    case dirFoundInStore:
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
   * @param path path to create
   * @return true if dir created/found
   * @throws IOException IO Failure.
   */
  private DirMapState maybeCreateOneDirectory(final Path path) throws IOException {
    LOG.info("Creating directory {}", path);
    // if a directory is in the map: return.
    final DirMapState dirMapState = dirMap.get(path);
    if (dirMapState != null) {
      LOG.debug("{}: Directory {} found in state {}; no need to create",
          getName(), path, dirMapState);
      // already exists in this job
      return DirMapState.dirFoundInMap;
    }

    // this call is 10x faster than mkdirs for GCS, so issue
    // it first
    FileStatus st = getFileStatusOrNull(path);
    if (st != null) {
      if (st.isDirectory()) {
        // is good.
        return DirMapState.dirFoundInStore;
      } else {
        // is bad: delete a file
        LOG.info("{}: Deleting file where a directory should go: {}",
            getName(), st);
        delete(path, false, OP_DELETE_FILE_UNDER_DESTINATION);
      }
    }

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

    // no mkdirs. Assume here that there's a race between two calls each of which
    // may take tens of milliseconds.
    // so look in the map to see if it is now present
    if (dirMap.containsKey(path)) {
      LOG.debug("{}: directory {} created/registered in another thread", getName(), path);
      return DirMapState.dirFoundInMap;
    }

    // fallback to checking the FS, in case a different process did it.
    st = getFileStatusOrNull(path);
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
   * Add a dir and all parents to the directory map.
   * @param dir directory.
   * @param state state of entry
   */
  private void addToDirectoryMap(final Path dir,
      DirMapState state) {
    dirMap.put(dir, state);
    addParentsToDirectoryMap(dir);
  }

  /**
   * Add parents of a dir to the directory map.
   * @param dir directory.
   */
  private void addParentsToDirectoryMap(final Path dir) {
    Path parent = dir.getParent();
    while (parent != null && !parent.isRoot() && !getDestinationDir().equals(parent)) {
      if (dirMap.get(parent) == null) {
        dirMap.put(parent, DirMapState.parentOfCreatedDir);
        parent = parent.getParent();
      } else {
        // it's in the map, so stop worrying
        break;
      }
    }
  }

  /**
   * Result of the operation.
   */
  public static final class Result {

    /** directories created. */
    private final List<Path> createdDirectories;

    /**
     * Map of dirs built up during preparation.
     */
    private final Map<Path, DirMapState> dirMap;

    public Result(List<Path> createdDirectories,
        Map<Path, DirMapState> dirMap) {
      this.createdDirectories = requireNonNull(createdDirectories);
      this.dirMap = requireNonNull(dirMap);
    }

    public List<Path> getCreatedDirectories() {
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
    ancestorWasFileNowDeleted,
    ancestorWasDirOrMissing,
    parentWasNotFile,
    parentOfCreatedDir
  }

}
