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

package org.apache.hadoop.fs.s3a.commit.staging.integration;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitMRJob;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.test.LambdaTestUtils;

/**
 * This is a test to verify that the committer will fail if the destination
 * directory exists, and that this happens in job setup.
 */
public final class ITestStagingCommitMRJobBadDest extends AbstractITCommitMRJob {

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

  @Override
  protected String committerName() {
    return StagingCommitter.NAME;
  }

  /**
   * create the destination directory and expect a failure.
   * @param conf configuration
   */
  @Override
  protected void applyCustomConfigOptions(JobConf conf) throws IOException {
    // This is the destination in the S3 FS
    String outdir = conf.get(FileOutputFormat.OUTDIR);
    S3AFileSystem fs = getFileSystem();
    Path outputPath = new Path(outdir);
    fs.mkdirs(outputPath);
  }

  @Override
  public void testMRJob() throws Exception {
    LambdaTestUtils.intercept(FileAlreadyExistsException.class,
        "Output directory",
        super::testMRJob);
  }

}
