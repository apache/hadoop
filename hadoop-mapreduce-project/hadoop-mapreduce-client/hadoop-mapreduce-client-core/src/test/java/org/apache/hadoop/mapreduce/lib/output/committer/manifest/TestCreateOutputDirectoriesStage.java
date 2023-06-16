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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_PREPARE_DIR_ANCESTORS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test directory creation.
 * As the directory creation phase relies on input from the task manifest to
 * determine which directories to explicitly create, delete files at
 * etc, these tests build up manifests and assert that the output
 * of the directory creation stage matches that of the combination
 * of the manifest and the filesystem state.
 */
public class TestCreateOutputDirectoriesStage extends AbstractManifestCommitterTest {

  /**
   * Deep tree width, subclasses (including in external projects)
   * may change.
   */
  protected static final int DEEP_TREE_WIDTH = 4;

  /**
   * The number of directories created in test setup; this must be
   * added to all assertions of the value of OP_MKDIRS.
   */
  private static final int DIRECTORIES_CREATED_IN_SETUP = 2;

  private Path destDir;
  private CreateOutputDirectoriesStage mkdirStage;
  private StageConfig stageConfig;
  private IOStatisticsStore iostats;

  @Override
  public void setup() throws Exception {
    super.setup();
    destDir = methodPath();
    // clean up dest dir completely
    destDir.getFileSystem(getConfiguration()).delete(destDir, true);
    setStoreOperations(createManifestStoreOperations());
    stageConfig = createStageConfigForJob(JOB1, destDir)
        .withDeleteTargetPaths(true);
    setJobStageConfig(stageConfig);
    // creates the job directories.
    new SetupJobStage(stageConfig).apply(true);
    mkdirStage = new CreateOutputDirectoriesStage(stageConfig);
    iostats = stageConfig.getIOStatistics();
    // assert original count of dirs created == 2 : job and task manifest
    verifyStatisticCounterValue(iostats, OP_MKDIRS,
        DIRECTORIES_CREATED_IN_SETUP);
    // reset the value to simplify future work
    iostats.getCounterReference(OP_MKDIRS).set(0);
  }

  @Test
  public void testPrepareSomeDirs() throws Throwable {

    final long initialFileStatusCount = lookupCounterStatistic(iostats, OP_GET_FILE_STATUS);
    final int dirCount = 8;

    // add duplicate entries to the list even though in the current iteration
    // that couldn't happen.
    final List<Path> dirs = subpaths(destDir, dirCount);
    final List<DirEntry> dirEntries = dirEntries(dirs, 1, EntryStatus.not_found);
    dirEntries.addAll(dirEntries(dirs, 1, EntryStatus.not_found));

    final CreateOutputDirectoriesStage.Result result = mkdirStage.apply(dirEntries);
    Assertions.assertThat(result.getCreatedDirectories())
        .describedAs("output of %s", mkdirStage)
        .containsExactlyInAnyOrderElementsOf(dirs);

    LOG.info("Job Statistics\n{}", ioStatisticsToPrettyString(iostats));

    // now dirCount new dirs are added.
    verifyStatisticCounterValue(iostats, OP_MKDIRS, dirCount);

    // now rerun the same preparation sequence, but this
    // time declare that the directories exist (as they do)
    final CreateOutputDirectoriesStage s2 =
        new CreateOutputDirectoriesStage(stageConfig);
    final CreateOutputDirectoriesStage.Result r2 = s2.apply(
        dirEntries(dirs, 1, EntryStatus.dir));

    // no directories are now created.
    Assertions.assertThat(r2.getCreatedDirectories())
        .describedAs("output of %s", s2)
        .isEmpty();
    LOG.info("Job Statistics after second pass\n{}", ioStatisticsToPrettyString(iostats));

    // second run probed no dest dirs
    verifyStatisticCounterValue(iostats, OP_GET_FILE_STATUS, initialFileStatusCount);
    // and no new mkdir calls were made
    verifyStatisticCounterValue(iostats, OP_MKDIRS, dirCount);
    verifyStatisticCounterValue(iostats, OP_DELETE_FILE_UNDER_DESTINATION, 0);
    verifyStatisticCounterValue(iostats, OP_IS_FILE, 0);
  }

  /**
   * Given a list of paths, build a list of DirEntry entries.
   * @param paths list of paths
   * @param level Level in the treewalk.
   * @param entryStatus status of dirs
   * @return list of entries with  the given level and entry status.
   */
  protected List<DirEntry> dirEntries(Collection<Path> paths,
      int level,
      EntryStatus entryStatus) {
    return paths.stream()
        .map(p -> DirEntry.dirEntry(p, entryStatus, level))
        .collect(Collectors.toList());
  }

  /**
   * Assert the directory map status of a path.
   * @param result stage result
   * @param path path to look up
   * @param expected expected value.
   */
  private static void assertDirMapStatus(
      CreateOutputDirectoriesStage.Result result,
      Path path,
      CreateOutputDirectoriesStage.DirMapState expected) {
    Assertions.assertThat(result.getDirMap())
        .describedAs("Directory Map entry for %s", path)
        .isNotNull()
        .containsKey(path)
        .containsEntry(path, expected);
  }

  /**
   * Prepare a deep tree {@code c ^ 3} of entries.
   * Make one of the parent dirs a file.
   *
   * From a test-purity perspective, this should
   * be separate tests. But attempting
   * operations in the same test cases spreads the
   * directory setup costs across both, rather than
   * duplicating it.
   */
  @Test
  public void testPrepareDirtyTree() throws Throwable {

    // build the lists of paths for the different levels
    final int c = getDeepTreeWidth();
    final List<Path> level1 = subpaths(destDir, c);
    final List<Path> level2 = level1.stream().flatMap(p ->
            subpaths(p, c).stream())
        .collect(Collectors.toList());
    final List<Path> level3 = level2.stream().flatMap(p ->
            subpaths(p, c).stream())
        .collect(Collectors.toList());
    // manifest dir entry list contains all levels > 0
    // adding them out of order verifies sorting takes place
    // before the merge routine which is intended to strip
    // out parent dirs
    final List<DirEntry> directories = new ArrayList<>();
    final List<DirEntry> l1 = dirEntries(level1, 1, EntryStatus.not_found);
    directories.addAll(l1);
    final List<DirEntry> l3 = dirEntries(level3, 3, EntryStatus.not_found);
    directories.addAll(l3);
    final List<DirEntry> l2 = dirEntries(level2, 2, EntryStatus.not_found);
    directories.addAll(l2);

    // one of the level 0 paths is going to be a file
    final DirEntry parentIsFile = l1.get(1);
    // one entry has a dir already
    final DirEntry parentIsDir = l2.get(0);
    // and one of the dest dirs is a file.
    final DirEntry leafIsFile = l3.get(0);

    // prepare the output
    CompletableFuture.allOf(
        asyncPut(parentIsFile.getDestPath(), NO_DATA),
        asyncPut(leafIsFile.getDestPath(), NO_DATA),
        asyncMkdir(parentIsDir.getDestPath()))
        .join();

    // patch the entries, which, as they are references
    // into the lists, updates the values there.
    parentIsFile.setStatus(EntryStatus.file);
    parentIsDir.setStatus(EntryStatus.dir);
    leafIsFile.setStatus(EntryStatus.file);

    // first attempt will succeed.
    final CreateOutputDirectoriesStage.Result result =
        mkdirStage.apply(directories);

    LOG.info("Job Statistics\n{}", ioStatisticsToPrettyString(iostats));

    assertDirMapStatus(result, leafIsFile.getDestPath(),
        CreateOutputDirectoriesStage.DirMapState.fileNowDeleted);

    // for the parent dir, all is good
    assertDirMapStatus(result, parentIsFile.getDestPath(),
        CreateOutputDirectoriesStage.DirMapState.fileNowDeleted);
    Assertions.assertThat(result.getCreatedDirectories())
        .describedAs("output of %s", mkdirStage)
        .containsExactlyInAnyOrderElementsOf(level3);
    verifyStatisticCounterValue(iostats, OP_MKDIRS, level3.size());
    // do a rerun. where the directory setup will fail because
    // a directory is present where the manifest says there is
    // a file.
    CreateOutputDirectoriesStage attempt2 =
        new CreateOutputDirectoriesStage(
            createStageConfigForJob(JOB1, destDir)
                .withDeleteTargetPaths(true));
    // attempt will fail because one of the entries marked as
    // a file to delete is now a non-empty directory
    LOG.info("Executing failing attempt to create the directories");
    intercept(IOException.class, () -> attempt2.apply(directories));
    verifyStatisticCounterValue(iostats, OP_PREPARE_DIR_ANCESTORS + SUFFIX_FAILURES, 1);
    verifyStatisticCounterValue(iostats, OP_DELETE + SUFFIX_FAILURES, 1);

    // build a new directory list where everything is declared a directory;
    // no dirs will be created this time as they all exist.
    final List<DirEntry> directories3 = new ArrayList<>();
    directories3.addAll(dirEntries(level1, 1, EntryStatus.dir));
    directories3.addAll(dirEntries(level2, 2, EntryStatus.dir));
    directories3.addAll(dirEntries(level3, 3, EntryStatus.dir));

    CreateOutputDirectoriesStage attempt3 =
        new CreateOutputDirectoriesStage(
            createStageConfigForJob(JOB1, destDir)
                .withDeleteTargetPaths(true));
    final CreateOutputDirectoriesStage.Result r3 =
        attempt3.apply(directories3);
    assertDirMapStatus(r3, leafIsFile.getDestPath(),
        CreateOutputDirectoriesStage.DirMapState.dirFoundInStore);
    Assertions.assertThat(r3.getCreatedDirectories())
        .describedAs("created directories")
        .isEmpty();
  }

  /**
   * Get the width of the deep tree; subclasses may tune for test performance, though
   * a wide one is more realistic of real jobs.
   * @return number of subdirs to create at each level. Must be at least 2
   */
  protected int getDeepTreeWidth() {
    return DEEP_TREE_WIDTH;
  }
}
