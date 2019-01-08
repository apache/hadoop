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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.hasItem;

/**
 * Test partitioned staging committer's logic for putting data in the right
 * place.
 */
public class TestStagingPartitionedFileListing
    extends TaskCommitterTest<PartitionedStagingCommitter> {

  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedStagingCommitter(OUTPUT_PATH,
        createTaskAttemptForJob());
  }

  @Override
  PartitionedStagingCommitter newTaskCommitter() throws IOException {
    return new PartitionedStagingCommitter(OUTPUT_PATH, getTAC());
  }

  private FileSystem attemptFS;
  private Path attemptPath;

  @After
  public void cleanupAttempt() {
    cleanup("teardown", attemptFS, attemptPath);
  }

  @Test
  public void testTaskOutputListing() throws Exception {
    PartitionedStagingCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    attemptPath = committer.getTaskAttemptPath(getTAC());
    attemptFS = attemptPath.getFileSystem(
        getTAC().getConfiguration());
    attemptFS.delete(attemptPath, true);

    try {
      List<String> expectedFiles = Lists.newArrayList();
      for (String dateint : Arrays.asList("20161115", "20161116")) {
        for (String hour : Arrays.asList("13", "14")) {
          String relative = "dateint=" + dateint + "/hour=" + hour +
              "/" + UUID.randomUUID().toString() + ".parquet";
          expectedFiles.add(relative);
          attemptFS.create(new Path(attemptPath, relative)).close();
        }
      }

      List<String> actualFiles = committer.getTaskOutput(getTAC())
          .stream()
          .map(stat -> Paths.getRelativePath(attemptPath,
              stat.getPath()))
          .collect(Collectors.toList());
      Collections.sort(expectedFiles);
      Collections.sort(actualFiles);
      assertEquals("File sets should match", expectedFiles, actualFiles);
    } finally {
      deleteQuietly(attemptFS, attemptPath, true);
    }

  }

  @Test
  public void testTaskOutputListingWithHiddenFiles() throws Exception {
    PartitionedStagingCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    attemptPath = committer.getTaskAttemptPath(getTAC());
    attemptFS = attemptPath.getFileSystem(
        getTAC().getConfiguration());
    attemptFS.delete(attemptPath, true);

    try {
      List<String> expectedFiles = Lists.newArrayList();
      for (String dateint : Arrays.asList("20161115", "20161116")) {
        String metadata = "dateint=" + dateint + "/" + "_metadata";
        attemptFS.create(new Path(attemptPath, metadata)).close();

        for (String hour : Arrays.asList("13", "14")) {
          String relative = "dateint=" + dateint + "/hour=" + hour +
              "/" + UUID.randomUUID().toString() + ".parquet";
          expectedFiles.add(relative);
          attemptFS.create(new Path(attemptPath, relative)).close();

          String partial = "dateint=" + dateint + "/hour=" + hour +
              "/." + UUID.randomUUID().toString() + ".partial";
          attemptFS.create(new Path(attemptPath, partial)).close();
        }
      }

      List<String> actualFiles = committer.getTaskOutput(getTAC()).stream()
          .map(stat -> Paths.getRelativePath(attemptPath, stat.getPath()))
          .collect(Collectors.toList());
      Collections.sort(expectedFiles);
      Collections.sort(actualFiles);
      assertEquals("File sets should match", expectedFiles, actualFiles);
    } finally {
      deleteQuietly(attemptFS, attemptPath, true);
    }

  }

  @Test
  public void testPartitionsResolution() throws Throwable {

    File tempDir = getTempDir();
    File partitionsDir = new File(tempDir, "partitions");

    attemptPath = new Path(partitionsDir.toURI());
    attemptFS = FileSystem.getLocal(getJob().getConfiguration());
    deleteQuietly(attemptFS, attemptPath, true);
    attemptFS.mkdirs(attemptPath);
    // initial partitioning -> empty
    assertTrue(Paths.getPartitions(attemptPath, new ArrayList<>(0)).isEmpty());
    String oct2017 = "year=2017/month=10";
    Path octLog = new Path(attemptPath, oct2017 + "/log-2017-10-04.txt");
    touch(attemptFS, octLog);
    assertThat(listPartitions(attemptFS, attemptPath), hasItem(oct2017));

    // add a root entry and it ends up under the table_root entry
    Path rootFile = new Path(attemptPath, "root.txt");
    touch(attemptFS, rootFile);
    assertThat(listPartitions(attemptFS, attemptPath),
        allOf(hasItem(oct2017),
            hasItem(StagingCommitterConstants.TABLE_ROOT)));
  }

  /**
   * List files in a filesystem using {@code listFiles()},
   * then get all the partitions.
   * @param fs filesystem
   * @param base base of tree
   * @return a list of partitions
   * @throws IOException failure
   */
  private Set<String> listPartitions(FileSystem fs, Path base)
      throws IOException {
    List<FileStatus> statusList = mapLocatedFiles(
        fs.listFiles(base, true), s -> (FileStatus) s);
    return Paths.getPartitions(base, statusList);
  }

}
