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
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.ClientErrors;
import org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.ClientResults;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Committer subclass that uses a mocked S3A connection for testing.
 */
class MockedStagingCommitter extends StagingCommitter {

  MockedStagingCommitter(Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  /**
   * Returns the mock FS without checking FS type.
   * @param out output path
   * @param config job/task config
   * @return a filesystem.
   * @throws IOException IO failure
   */
  @Override
  protected FileSystem getDestinationFS(Path out, Configuration config)
      throws IOException {
    return out.getFileSystem(config);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    // turn off stamping an output marker, as that codepath isn't mocked yet.
    super.commitJob(context);
    Configuration conf = context.getConfiguration();
    try {
      String jobCommitterPath = conf.get("mock-results-file");
      if (jobCommitterPath != null) {
        try (ObjectOutputStream out = new ObjectOutputStream(
            FileSystem.getLocal(conf)
                .create(new Path(jobCommitterPath), false))) {
          out.writeObject(getResults());
        }
      }
    } catch (Exception e) {
      // do nothing, the test will fail
    }
  }

  @Override
  protected void maybeCreateSuccessMarker(JobContext context,
      List<String> filenames,
      final IOStatisticsSnapshot ioStatistics)
      throws IOException {
     //skipped
  }

  public ClientResults getResults() throws IOException {
    MockS3AFileSystem mockFS = (MockS3AFileSystem)getDestS3AFS();
    return mockFS.getOutcome().getKey();
  }

  public ClientErrors getErrors() throws IOException {
    MockS3AFileSystem mockFS = (MockS3AFileSystem) getDestS3AFS();
    return mockFS.getOutcome().getValue();
  }

  @Override
  public String toString() {
    return "MockedStagingCommitter{ " + super.toString() + " ";
  }
}
