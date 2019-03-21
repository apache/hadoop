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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitMRJob;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.mapred.JobConf;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Full integration test for the Magic Committer.
 *
 * There's no need to disable the committer setting for the filesystem here,
 * because the committers are being instantiated in their own processes;
 * the settings in {@link AbstractITCommitMRJob#applyCustomConfigOptions(JobConf)} are
 * passed down to these processes.
 */
public final class ITestMagicCommitMRJob extends AbstractITCommitMRJob {

  /**
   * The static cluster binding with the lifecycle of this test; served
   * through instance-level methods for sharing across methods in the
   * suite.
   */
  @SuppressWarnings("StaticNonFinalField")
  private static ClusterBinding clusterBinding;

  @BeforeClass
  public static void setupClusters() throws IOException {
    clusterBinding = createCluster(new JobConf());
  }

  @AfterClass
  public static void teardownClusters() throws IOException {
    clusterBinding.terminate();
  }

  @Override
  public ClusterBinding getClusterBinding() {
    return clusterBinding;
  }

  /**
   * Need consistency here.
   * @return false
   */
  @Override
  public boolean useInconsistentClient() {
    return false;
  }

  @Override
  protected String committerName() {
    return MagicS3GuardCommitter.NAME;
  }

  /**
   * Turn on the magic commit support for the FS, else nothing will work.
   * @param conf configuration
   */
  @Override
  protected void applyCustomConfigOptions(JobConf conf) {
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
  }

  /**
   * Check that the magic dir was cleaned up.
   * {@inheritDoc}
   */
  @Override
  protected void customPostExecutionValidation(Path destPath,
      SuccessData successData) throws Exception {
    assertPathDoesNotExist("No cleanup", new Path(destPath, MAGIC));
  }
}
