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


package org.apache.hadoop.fs.s3a.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.impl.CommitUtilsWithMR;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTrackerUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.apache.hadoop.fs.s3a.commit.AbstractCommitITest.randomJobId;

/**
 * Class to test {@link MagicCommitTrackerUtils}.
 */
public final class TestMagicCommitTrackerUtils {

  private String jobId;
  private String attemptId;
  private TaskAttemptID taskAttemptId;
  private static final Path DEST_PATH = new Path("s3://dummyBucket/dummyTable");


  @BeforeEach
  public void setup() throws Exception {
    jobId = randomJobId();
    attemptId = "attempt_" + jobId + "_m_000000_0";
    taskAttemptId = TaskAttemptID.forName(attemptId);
  }

  @Test
  public void testExtractTaskAttemptIdFromPath() {
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(
        new Configuration(),
        taskAttemptId);
    Path path = CommitUtilsWithMR
        .getBaseMagicTaskAttemptPath(taskAttemptContext, "00001", DEST_PATH);
    assertEquals(attemptId,
        MagicCommitTrackerUtils.extractTaskAttemptIdFromPath(path),
        "TaskAttemptId didn't match");

  }
}
