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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test directory creation.
 * There's issues here with concurrency, but also that the
 * state of the directory is unknown and could be considered
 * inconsistent with the current job.
 *
 * Bad
 * - file ancestor of target dir
 * - file is target dir
 * Good
 * - dir exists, empty
 * - dir exists, not empty
 * plus concurrency.
 */
public class TestCreateOutputDirectoriesStage extends AbstractManifestCommitterTest {

  /**
   * Deep tree width, subclasses (including in external projects)
   * may change.
   */
  protected static final int DEEP_TREE_WIDTH = 4;

  /**
   * Fault Injection.
   */
  private UnreliableManifestStoreOperations failures;
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
    failures
        = new UnreliableManifestStoreOperations(createManifestStoreOperations());
    setStoreOperations(failures);
    stageConfig = createStageConfigForJob(JOB1, destDir)
        .withPrepareParentDirectories(false)
        .withDeleteTargetPaths(true);
    setJobStageConfig(stageConfig);
    // creates the job directories.
    new SetupJobStage(stageConfig).apply(true);
    mkdirStage = new CreateOutputDirectoriesStage(stageConfig);
    iostats = stageConfig.getIOStatistics();
  }

  @Test
  public void testPrepareSomeDirs() throws Throwable {
    // assert original count of dirs created == 2 : job and task manifest
    final int directoriesCreatedInSetup = 2;
    verifyStatisticCounterValue(iostats, OP_MKDIRS, directoriesCreatedInSetup);
    final long initialFileStatusCount = lookupCounterStatistic(iostats, OP_GET_FILE_STATUS);
    final int dirCount = 8;
    final List<Path> dirs = subpaths(destDir, dirCount);
    final List<DirEntry> dirEntries = dirEntries(dirs);

    // two manifests with duplicate entries
    final List<TaskManifest> manifests = Lists.newArrayList(
        manifestWithDirsToCreate(dirEntries),
        manifestWithDirsToCreate(dirEntries));
    final CreateOutputDirectoriesStage.Result result = mkdirStage.apply(manifests);
    Assertions.assertThat(result.getCreatedDirectories())
        .describedAs("output of %s", mkdirStage)
        .containsExactlyInAnyOrderElementsOf(dirs);

    LOG.info("Job Statistics\n{}", ioStatisticsToPrettyString(iostats));

    // now dirCount new dirs are added.
    verifyStatisticCounterValue(iostats, OP_MKDIRS, directoriesCreatedInSetup + dirCount);

    // now rerun the same preparation sequence
    final CreateOutputDirectoriesStage s2 =
        new CreateOutputDirectoriesStage(stageConfig);
    final CreateOutputDirectoriesStage.Result r2 = s2.apply(manifests);

    // mkdirs() is called the same number of times, because there's no
    // check for existence
    Assertions.assertThat(r2.getCreatedDirectories())
        .describedAs("output of %s", s2)
        .containsExactlyInAnyOrderElementsOf(dirs);
    LOG.info("Job Statistics after second pass\n{}", ioStatisticsToPrettyString(iostats));

    // both runs probed all dest dirs
    verifyStatisticCounterValue(iostats, OP_GET_FILE_STATUS, initialFileStatusCount + dirCount * 2);
    // but no new mkdir calls were made the second time
    verifyStatisticCounterValue(iostats, OP_MKDIRS, directoriesCreatedInSetup + dirCount);
    verifyStatisticCounterValue(iostats, OP_DELETE_FILE_UNDER_DESTINATION, 0);
    verifyStatisticCounterValue(iostats, OP_IS_FILE, 0);
  }

  /**
   * Given a list of paths, build a list of FileOrDirEntry entries.
   * @param paths list of paths
   * @return list of entries where src == dest.
   */
  protected List<DirEntry> dirEntries(Collection<Path> paths) {
    return paths.stream()
        .map(p -> DirEntry.dirEntry(p, 0))
        .collect(Collectors.toList());
  }

  protected TaskManifest manifestWithDirsToCreate(List<DirEntry> dirEntries) {
    final TaskManifest taskManifest = new TaskManifest();
    taskManifest.getDirectoriesToCreate().addAll(dirEntries);
    return taskManifest;
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
   * Make one of the parent dirs a file and so expect
   * the first attempt to initially fail as the stage configuration
   * does not parent dir preparation.
   * The second attempt will, so succeeds.
   *
   * From a test-purity perspective, this should
   * be separate tests. But attempting both
   * operations in the same test cases spreads the
   * directory setup costs across both, rather than
   * duplicating it.
   */
  @Test
  public void testPrepareDeepTree() throws Throwable {

    // build the lists of paths for the different levels
    final int c = getDeepTreeWidth();
    final List<Path> level0 = subpaths(destDir, c);
    final List<Path> level1 = level0.stream().flatMap(p ->
            subpaths(p, c).stream())
        .collect(Collectors.toList());
    final List<Path> level2 = level1.stream().flatMap(p ->
            subpaths(p, c).stream())
        .collect(Collectors.toList());

    // one of the level 0 paths is going to be a file
    final Path parentIsFile = level0.get(1);
    // one entry has a dir already
    final Path parentIsDir = level1.get(0);
    // and one of the dest dirs is a file.
    final Path destIsFile = level2.get(0);

    // prepare the output
    CompletableFuture.allOf(
        asyncPut(parentIsFile, NO_DATA),
        asyncPut(destIsFile, NO_DATA),
        asyncMkdir(parentIsDir))
        .join();

    // manifest dir entry list is only that bottom list
    final List<DirEntry> dirEntries = dirEntries(level2);


    final List<TaskManifest> manifests = Lists.newArrayList(
        manifestWithDirsToCreate(dirEntries));

    // first attempt will fail because of the parent dir & this job is set
    // to not try and delete
    LOG.info("Executing failing attempt to create the directories");
    intercept(IOException.class, () -> mkdirStage.apply(manifests));

    // mkdirs failed for the file
    final String failuresKey = OP_MKDIRS + SUFFIX_FAILURES;
    final long initialFailureCount = iostats.counters().get(failuresKey);
    Assertions.assertThat(initialFailureCount)
        .describedAs("value of %s", failuresKey)
        .isGreaterThanOrEqualTo(0);

    // create a job configured to clean up first
    CreateOutputDirectoriesStage attempt2
        = new CreateOutputDirectoriesStage(
        createStageConfigForJob(JOB1, destDir)
            .withPrepareParentDirectories(true)
            .withDeleteTargetPaths(true));
    LOG.info("Executing failing attempt to create the directories");

    final CreateOutputDirectoriesStage.Result result = attempt2.apply(manifests);
    LOG.info("Job Statistics\n{}", ioStatisticsToPrettyString(iostats));

    assertDirMapStatus(result, destIsFile,
        CreateOutputDirectoriesStage.DirMapState.dirWasCreated);

    // for the parent dir, all is good
    assertDirMapStatus(result, parentIsFile,
        CreateOutputDirectoriesStage.DirMapState.ancestorWasFileNowDeleted);
    Assertions.assertThat(result.getCreatedDirectories())
        .describedAs("output of %s", mkdirStage)
        .containsExactlyInAnyOrderElementsOf(level2);
    verifyStatisticCounterValue(iostats, failuresKey, initialFailureCount);

    // and now, do a rerun. where everything is good.
    CreateOutputDirectoriesStage attempt3 =
        new CreateOutputDirectoriesStage(
            createStageConfigForJob(JOB1, destDir)
                .withPrepareParentDirectories(true)
                .withDeleteTargetPaths(true));
    assertDirMapStatus(attempt3.apply(manifests), destIsFile,
        CreateOutputDirectoriesStage.DirMapState.dirFoundInStore);
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
