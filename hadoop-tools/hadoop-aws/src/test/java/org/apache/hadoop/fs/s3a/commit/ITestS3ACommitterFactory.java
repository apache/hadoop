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

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Tests for some aspects of the committer factory.
 * All tests are grouped into one single test so that only one
 * S3A FS client is set up and used for the entire run.
 * Saves time and money.
 */
public class ITestS3ACommitterFactory extends AbstractCommitITest {


  protected static final String INVALID_NAME = "invalid-name";

  /**
   * Counter to guarantee that even in parallel test runs, no job has the same
   * ID.
   */

  private String jobId;

  // A random task attempt id for testing.
  private String attempt0;

  private TaskAttemptID taskAttempt0;

  private Path outDir;

  private S3ACommitterFactory factory;

  private TaskAttemptContext tContext;

  /**
   * Parameterized list of bindings of committer name in config file to
   * expected class instantiated.
   */
  private static final Object[][] bindings = {
      {COMMITTER_NAME_FILE, FileOutputCommitter.class},
      {COMMITTER_NAME_DIRECTORY, DirectoryStagingCommitter.class},
      {COMMITTER_NAME_PARTITIONED, PartitionedStagingCommitter.class},
      {InternalCommitterConstants.COMMITTER_NAME_STAGING,
          StagingCommitter.class},
      {COMMITTER_NAME_MAGIC, MagicS3GuardCommitter.class}
  };

  /**
   * This is a ref to the FS conf, so changes here are visible
   * to callers querying the FS config.
   */
  private Configuration filesystemConfRef;

  private Configuration taskConfRef;

  @Override
  public void setup() throws Exception {
    super.setup();
    jobId = randomJobId();
    attempt0 = "attempt_" + jobId + "_m_000000_0";
    taskAttempt0 = TaskAttemptID.forName(attempt0);

    outDir = path(getMethodName());
    factory = new S3ACommitterFactory();
    Configuration conf = new Configuration();
    conf.set(FileOutputFormat.OUTDIR, outDir.toUri().toString());
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    filesystemConfRef = getFileSystem().getConf();
    tContext = new TaskAttemptContextImpl(conf, taskAttempt0);
    taskConfRef = tContext.getConfiguration();
  }

  @Test
  public void testEverything() throws Throwable {
    testImplicitFileBinding();
    testBindingsInTask();
    testBindingsInFSConfig();
    testInvalidFileBinding();
    testInvalidTaskBinding();
  }

  /**
   * Verify that if all config options are unset, the FileOutputCommitter
   *
   * is returned.
   */
  public void testImplicitFileBinding() throws Throwable {
    taskConfRef.unset(FS_S3A_COMMITTER_NAME);
    filesystemConfRef.unset(FS_S3A_COMMITTER_NAME);
    assertFactoryCreatesExpectedCommitter(FileOutputCommitter.class);
  }

  /**
   * Verify that task bindings are picked up.
   */
  public void testBindingsInTask() throws Throwable {
    // set this to an invalid value to be confident it is not
    // being checked.
    filesystemConfRef.set(FS_S3A_COMMITTER_NAME, "INVALID");
    taskConfRef.set(FS_S3A_COMMITTER_NAME, COMMITTER_NAME_FILE);
    assertFactoryCreatesExpectedCommitter(FileOutputCommitter.class);
    for (Object[] binding : bindings) {
      taskConfRef.set(FS_S3A_COMMITTER_NAME,
          (String) binding[0]);
      assertFactoryCreatesExpectedCommitter((Class) binding[1]);
    }
  }

  /**
   * Verify that FS bindings are picked up.
   */
  public void testBindingsInFSConfig() throws Throwable {
    taskConfRef.unset(FS_S3A_COMMITTER_NAME);
    filesystemConfRef.set(FS_S3A_COMMITTER_NAME, COMMITTER_NAME_FILE);
    assertFactoryCreatesExpectedCommitter(FileOutputCommitter.class);
    for (Object[] binding : bindings) {
      taskConfRef.set(FS_S3A_COMMITTER_NAME, (String) binding[0]);
      assertFactoryCreatesExpectedCommitter((Class) binding[1]);
    }
  }

  /**
   * Create an invalid committer via the FS binding,
   */
  public void testInvalidFileBinding() throws Throwable {
    taskConfRef.unset(FS_S3A_COMMITTER_NAME);
    filesystemConfRef.set(FS_S3A_COMMITTER_NAME, INVALID_NAME);
    LambdaTestUtils.intercept(PathCommitException.class, INVALID_NAME,
        () -> createCommitter());
  }

  /**
   * Create an invalid committer via the task attempt.
   */
  public void testInvalidTaskBinding() throws Throwable {
    filesystemConfRef.unset(FS_S3A_COMMITTER_NAME);
    taskConfRef.set(FS_S3A_COMMITTER_NAME, INVALID_NAME);
    LambdaTestUtils.intercept(PathCommitException.class, INVALID_NAME,
        () -> createCommitter());
  }

  /**
   * Assert that the factory creates the expected committer.
   * @param expected expected committer class.
   * @throws IOException IO failure.
   */
  protected void assertFactoryCreatesExpectedCommitter(
      final Class expected)
      throws IOException {
    assertEquals("Wrong Committer from factory",
        expected,
        createCommitter().getClass());
  }

  /**
   * Create a committer.
   * @return the committer
   * @throws IOException IO failure.
   */
  private PathOutputCommitter createCommitter() throws IOException {
    return factory.createOutputCommitter(outDir, tContext);
  }
}
