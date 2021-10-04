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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MKDIRS_RETURNED_FALSE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_PREPARE_DIR_ANCESTORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;

/**
 * Prepare the destination directory tree, by making as few IO calls as
 * possible -and doing those IO operations in the thread pool.
 *
 * This is fairly complex as tries to emulate the classic FileOutputCommitter's
 * handling of files and dirs during its recursive treewalks.
 *
 * Each task manifest's directories are combined with those of the other tasks
 * to build a set of all directories which are needed, without duplicates.
 *
 * A ancestor directories is then also built up, all of which MUST NOT
 * be files and SHALL be in one of two states
 * - directory
 * - nonexistent
 * Across a thread pool, An isFile() probe is made for all parents;
 * if there is a file there then it is deleted.
 *
 * After ancestor dirs are created, the final dir set is created
 * though mkdirs() calls, with some handling for race conditions.
 * It is not an error if mkdirs() fails because the dest dir is there;
 * if it fails because a file was found -that file will be deleted.0
 *
 * The stage returns the list of directories created.
 */
public class CreateOutputDirectoriesStage extends
    AbstractJobCommitStage<List<TaskManifest>, CreateOutputDirectoriesStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CreateOutputDirectoriesStage.class);

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

    LOG.info("Creating output directories");
    final List<Path> directories = createAllDirectories(taskManifests);
    LOG.debug("Created {} directories", directories.size());
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

    // Go through dir map and insert an entry for all
    // ancestor dirs of the rename destination path
    // rename wants to rename a file to a/b/c/d.orc
    // is list of dirs to create is [a/b, a/b/c]
    // but a/b == exists at the dest and is a file

    // there are no duplicates: creating the set is sufficient
    // to filter all these out.

    // include parents in the paths
    final Set<Path> ancestors = extractAncestors(
        getStageConfig().getDestinationDir(),
        directoriesToCreate);

    // clean up parent dirs. No attempt is made to create them, because
    // we know mkdirs() will do that.
    trackDurationOfInvocation(getIOStatistics(), OP_PREPARE_DIR_ANCESTORS, () ->
        TaskPool.foreach(ancestors)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .run(dir -> {
              updateAuditContext(OP_PREPARE_DIR_ANCESTORS);
              LOG.debug("Probing parent dir {}", dir);
              if (isFile(dir)) {
                // it's a file: delete it.
                LOG.info("Deleting file {}", dir);
                delete(dir, false, OP_DELETE_FILE_UNDER_DESTINATION);
                // note its final state
                addToDirectoryMap(dir, DirMapState.ancestorWasFileNowDeleted);
              } else {
                // and add to dir map as a dir or missing entry
                LOG.debug("Dir {} is missing or a directory", dir);
                addToDirectoryMap(dir, DirMapState.ancestorWasDirOrMissing);
              }
            }));

    // now probe for and create the parent dirs of all renames
    trackDurationOfInvocation(getIOStatistics(), OP_CREATE_DIRECTORIES, () ->
        TaskPool.foreach(directoriesToCreate)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .run(dir -> {
              updateAuditContext(OP_STAGE_JOB_CREATE_TARGET_DIRS);
              if (maybeCreateOneDirectory(dir)) {
                addCreatedDirectory(dir);
              }
            }));
    return createdDirectories;
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
        LOG.warn("root directory {} is one of the destination directories", dir);
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



  /**
   * Try to efficiently create a directory in a method which is
   * expected to be executed in parallel with operations creating
   * peer directories.
   * @param path path to create
   * @return true if dir created/found
   * @throws IOException IO Failure.
   */
  private boolean maybeCreateOneDirectory(final Path path) throws IOException {
    LOG.info("Creating directory {}", path);
    // if a directory is in the map: return.
    final DirMapState dirMapState = dirMap.get(path);
    if (dirMapState != null) {
      LOG.info("Directory {} found in state {}; no need to create", path, dirMapState);
      // already exists in this job
      return false;
    }

    DirMapState outcome = DirMapState.dirWasCreated;

    // create the dir
    try {
      if (mkdirs(path, false)) {
        // add the dir to the map, along with all parents.
        addToDirectoryMap(path, DirMapState.dirWasCreated);
        return true;
      }
      getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);
      outcome = DirMapState.mkdirReturnedFalse;
      LOG.info("mkdirs({}) returned false, attempting to recover", path);
    } catch (IOException e) {
      // can be caused by file existing, etc.
      LOG.info("mkdir({}) raised exception {}", path, e.toString());
      LOG.debug("Mkdir stack", e);
      outcome = DirMapState.mkdirRaisedException;
    }

    // no mkdirs. Assume here that there's a problem.
    // See if it exists
    boolean create;
    final FileStatus st = getFileStatusOrNull(path);
    if (st != null) {
      if (!st.isDirectory()) {
        // is bad: delete a file
        LOG.info("Deleting file where a directory should go: {}", st);
        delete(path, false, OP_DELETE_FILE_UNDER_DESTINATION);
        create = true;
      } else {
        // is good.
        LOG.warn("Even though mkdirs({}) failed, there is a directory there", path);
        create = false;
      }
    } else {
      // nothing found. This should never happen as the first mkdirs should have created it.
      // so the getFileStatus call never reached.
      LOG.warn("Although mkdirs({}) returned false, there's nothing at that path to prevent it",
          path);
      create = true;
    }

    if (create) {
      // create the directory
      // if this fails, and IOE is still raised, well, that's bad news
      // which will be escalated as a failure
      if (!mkdirs(path, false)) {
        // two possible outcomes. Something went very wrong
        // or the directory was created by an action in a parallel thread

        getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);

        // require the dir to exist, raising an exception if it does not.
        directoryMustExist("Creating directory ", path);
        create = false;
      }
    }
    // and whatever that outcome was, record it
    addToDirectoryMap(path, outcome);
    return create;

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
    dirWasCreated,
    mkdirReturnedFalse,
    mkdirRaisedException,
    ancestorWasFileNowDeleted,
    ancestorWasDirOrMissing,
    parentWasNotFile,
    parentOfCreatedDir
  }

}
