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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;

/**
 * Test partitioned staging committer's logic for putting data in the right
 * place.
 */
public class TestStagingPartitionedFileListing
    extends TaskCommitterTest<PartitionedStagingCommitter> {

  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedStagingCommitter(OUTPUT_PATH, getJob());
  }

  @Override
  PartitionedStagingCommitter newTaskCommitter() throws IOException {
    return new PartitionedStagingCommitter(OUTPUT_PATH, getTAC());
  }

  @Test
  public void testTaskOutputListing() throws Exception {
    PartitionedStagingCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    Path attemptPath = committer.getTaskAttemptPath(getTAC());
    FileSystem attemptFS = attemptPath.getFileSystem(
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

      List<FileStatus> attemptFiles = committer.getTaskOutput(getTAC());
      List<String> actualFiles = Lists.newArrayList();
      for (FileStatus stat : attemptFiles) {
        String relative = Paths.getRelativePath(attemptPath, stat.getPath());
        actualFiles.add(relative);
      }
      Collections.sort(attemptFiles);
      Collections.sort(actualFiles);
      assertEquals("File sets should match", expectedFiles, actualFiles);
    } finally {
      attemptFS.delete(attemptPath, true);
    }

  }

  @Test
  public void testTaskOutputListingWithHiddenFiles() throws Exception {
    PartitionedStagingCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    Path attemptPath = committer.getTaskAttemptPath(getTAC());
    FileSystem attemptFS = attemptPath.getFileSystem(
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

      List<FileStatus> attemptFiles = committer.getTaskOutput(getTAC());
      List<String> actualFiles = Lists.newArrayList();
      for (FileStatus stat : attemptFiles) {
        String relative = Paths.getRelativePath(attemptPath, stat.getPath());
        actualFiles.add(relative);
      }
      Collections.sort(attemptFiles);
      Collections.sort(actualFiles);
      assertEquals("File sets should match", expectedFiles, actualFiles);
    } finally {
      attemptFS.delete(attemptPath, true);
    }

  }
}
