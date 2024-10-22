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
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.s3a.commit.optimized.OptimizedS3MagicCommitter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.COMMITTER_NAME_STAGING;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests for the committer factory creation/override process.
 */
@RunWith(Parameterized.class)
public final class ITestS3ACommitterFactory extends AbstractCommitITest {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3ACommitterFactory.class);
  /**
   * Name for invalid committer: {@value}.
   */
  private static final String INVALID_NAME = "invalid-name";

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
  private static final Object[][] BINDINGS = {
      {"", "", FileOutputCommitter.class, "Default Binding"},
      {COMMITTER_NAME_FILE, "", FileOutputCommitter.class, "File committer in FS"},
      {COMMITTER_NAME_PARTITIONED, "", PartitionedStagingCommitter.class,
          "partitoned committer in FS"},
      {COMMITTER_NAME_STAGING, "", StagingCommitter.class, "staging committer in FS"},
      {COMMITTER_NAME_MAGIC, "", MagicS3GuardCommitter.class, "magic committer in FS"},
      {COMMITTER_NAME_DIRECTORY, "", DirectoryStagingCommitter.class, "Dir committer in FS"},
      {INVALID_NAME, "", null, "invalid committer in FS"},

      {"", COMMITTER_NAME_FILE, FileOutputCommitter.class, "File committer in task"},
      {"", COMMITTER_NAME_PARTITIONED, PartitionedStagingCommitter.class,
          "partioned committer in task"},
      {"", COMMITTER_NAME_STAGING, StagingCommitter.class, "staging committer in task"},
      {"", COMMITTER_NAME_MAGIC, MagicS3GuardCommitter.class, "magic committer in task"},
      {"", COMMITTER_NAME_DIRECTORY, DirectoryStagingCommitter.class, "Dir committer in task"},
      {"", INVALID_NAME, null, "invalid committer in task"},
  };

  /**
   * Test array for parameterized test runs.
   *
   * @return the committer binding for this run.
   */
  @Parameterized.Parameters(name = "{3}-fs=[{0}]-task=[{1}]-[{2}]")
  public static Collection<Object[]> params() {
    return Arrays.asList(BINDINGS);
  }

  /**
   * Name of committer to set in filesystem config. If "" do not set one.
   */
  private final String fsCommitterName;

  /**
   * Name of committer to set in job config.
   */
  private final String jobCommitterName;

  /**
   * Expected committer class.
   * If null: an exception is expected
   */
  private final Class<? extends AbstractS3ACommitter> committerClass;

  /**
   * Description from parameters, simply for thread names to be more informative.
   */
  private final String description;

  /**
   * Create a parameterized instance.
   * @param fsCommitterName committer to set in filesystem config
   * @param jobCommitterName committer to set in job config
   * @param committerClass expected committer class
   * @param description debug text for thread names.
   */
  public ITestS3ACommitterFactory(
      final String fsCommitterName,
      final String jobCommitterName,
      final Class<? extends AbstractS3ACommitter> committerClass,
      final String description) {
    this.fsCommitterName = fsCommitterName;
    this.jobCommitterName = jobCommitterName;
    this.committerClass = committerClass;
    this.description = description;
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    // do not cache, because we want the committer one to pick up
    // the fs with fs-specific configuration
    conf.setBoolean(FS_S3A_IMPL_DISABLE_CACHE, false);
    removeBaseAndBucketOverrides(conf, FS_S3A_COMMITTER_NAME);
    maybeSetCommitterName(conf, fsCommitterName);
    return conf;
  }

  /**
   * Set a committer name in a configuration.
   * @param conf configuration to patch.
   * @param name name. If "" the option is unset.
   */
  private static void maybeSetCommitterName(final Configuration conf, final String name) {
    if (!name.isEmpty()) {
      conf.set(FS_S3A_COMMITTER_NAME, name);
    } else {
      conf.unset(FS_S3A_COMMITTER_NAME);
    }
  }

  @Override
  public void setup() throws Exception {
    // destroy all filesystems from previous runs.
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    super.setup();
    jobId = randomJobId();
    attempt0 = "attempt_" + jobId + "_m_000000_0";
    taskAttempt0 = TaskAttemptID.forName(attempt0);

    outDir = methodPath();
    factory = new S3ACommitterFactory();
    final Configuration fsConf = getConfiguration();
    JobConf jobConf = new JobConf(fsConf);
    jobConf.set(FileOutputFormat.OUTDIR, outDir.toUri().toString());
    jobConf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    maybeSetCommitterName(jobConf, jobCommitterName);
    tContext = new TaskAttemptContextImpl(jobConf, taskAttempt0);

    LOG.info("{}: Filesystem Committer='{}'; task='{}'",
        description,
        fsConf.get(FS_S3A_COMMITTER_NAME),
        jobConf.get(FS_S3A_COMMITTER_NAME));
  }


  @Override
  protected void deleteTestDirInTeardown() {
    // no-op
  }

  /**
   * Verify that if all config options are unset, the FileOutputCommitter
   * is returned.
   */
  @Test
  public void testBinding() throws Throwable {
    assertFactoryCreatesExpectedCommitter(committerClass);
  }

  /**
   * Assert that the factory creates the expected committer.
   * If a null committer is passed in, a {@link PathIOException}
   * is expected.
   * @param expected expected committer class.
   * @throws Exception IO failure.
   */
  private void assertFactoryCreatesExpectedCommitter(
      final Class expected)
      throws Exception {
    describe("Creating committer: expected class \"%s\"", expected);
    if (expected != null) {
      assertEquals("Wrong Committer from factory",
          expected,
          createCommitter().getClass());
    } else {
      intercept(PathCommitException.class, this::createCommitter);
    }
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
