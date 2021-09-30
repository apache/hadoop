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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry.dirEntry;

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
   * Number of task attempts to create. Manifests are created and written
   * as well as test dirs, but no actual files.
   */
  protected static final int TASK_ATTEMPT_COUNT = 10;

  /**
   * How many delete calls for the root job delete?
   */
  protected static final int ROOT_DELETE_COUNT = 1;

  /**
   * Tocal invocation count for a successful parallel delete job.
   */
  protected static final int PARALLEL_DELETE_COUNT =
      TASK_ATTEMPT_COUNT + ROOT_DELETE_COUNT;

  /**
   * Fault Injection.
   */
  private UnreliableStoreOperations failures;

  @Override
  public void setup() throws Exception {
    super.setup();
    failures
        = new UnreliableStoreOperations(createStoreOperations());
    setStoreOperations(failures);
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    setJobStageConfig(stageConfig);
    new SetupJobStage(stageConfig).apply(true);

  }

  @Test
  public void testPrepareSomeDirs() throws Throwable {
    final StageConfig stageConfig = getJobStageConfig();
    final IOStatisticsStore iostats = stageConfig.getIOStatistics();
    final Path destDir = stageConfig.getDestinationDir();
    // assert original count of dirs created == 1
    verifyStatisticCounterValue(iostats, OP_MKDIRS, 1);

    final int dirCount = 8;
    final List<Path> dirs = subpaths(destDir, dirCount);
    final List<FileOrDirEntry> dirEntries = dirEntries(dirs);
    final TaskManifest taskManifest = new TaskManifest();
    taskManifest.getDirectoriesToCreate().addAll(dirEntries);
    final TaskManifest taskManifest2 = new TaskManifest();
    taskManifest2.getDirectoriesToCreate().addAll(dirEntries);


    final CreateOutputDirectoriesStage stage
        = new CreateOutputDirectoriesStage(stageConfig);
    final ArrayList<TaskManifest> manifests = Lists.newArrayList(
        taskManifest,
        taskManifest2);
    final CreateOutputDirectoriesStage.Result result = stage.apply(manifests);
    Assertions.assertThat(result.getDirectories())
        .describedAs("output of %s", stage)
        .containsExactlyInAnyOrderElementsOf(dirs);

    LOG.info("Job Statistics\n{}", ioStatisticsToPrettyString(iostats));

    // now dirCount new dirs are added.
    verifyStatisticCounterValue(iostats, OP_MKDIRS, 1 + dirCount);

    // now rerun the same preparation sequence
    final CreateOutputDirectoriesStage s2 =
        new CreateOutputDirectoriesStage(stageConfig);
    final CreateOutputDirectoriesStage.Result r2 = s2.apply(manifests);

    // mkdirs() is called the same number of times, because there's no
    // check for existence
    Assertions.assertThat(r2.getDirectories())
        .describedAs("output of %s", s2)
        .containsExactlyInAnyOrderElementsOf(dirs);
    LOG.info("Job Statistics after second pass\n{}", ioStatisticsToPrettyString(iostats));

    verifyStatisticCounterValue(iostats, OP_MKDIRS, 1 + dirCount * 2);
    verifyStatisticCounterValue(iostats, OP_DELETE_FILE_UNDER_DESTINATION, 0);
    verifyStatisticCounterValue(iostats, OP_IS_FILE, 0);

  }

  /**
   * Given a list of paths, build a list of FileOrDirEntry entries.
   * @param paths list of paths
   * @return list of entries where src == dest.
   */
  List<FileOrDirEntry> dirEntries(Collection<Path> paths) {
    return paths.stream()
        .map(p -> dirEntry(p, p))
        .collect(Collectors.toList());
  }
}
