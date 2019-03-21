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

import org.hamcrest.core.StringContains;
import org.hamcrest.core.StringEndsWith;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.AbstractITCommitMRJob;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_TMP_PATH;
import static org.apache.hadoop.fs.s3a.commit.staging.Paths.getMultipartUploadCommitsDirectory;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.STAGING_UPLOADS;

/**
 * Full integration test for the staging committer.
 */
public final class ITestStagingCommitMRJob extends AbstractITCommitMRJob {

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
   * Verify that staging commit dirs are made absolute under the user's
   * home directory, so, in a secure cluster, private.
   */
  @Test
  public void testStagingDirectory() throws Throwable {
    FileSystem hdfs = getDFS();
    Configuration conf = hdfs.getConf();
    conf.set(FS_S3A_COMMITTER_STAGING_TMP_PATH, "private");
    Path dir = getMultipartUploadCommitsDirectory(conf, "UUID");
    assertThat("Directory " + dir + " path is wrong",
        dir.toString(),
        StringEndsWith.endsWith("UUID/"
        + STAGING_UPLOADS));
    assertTrue("path unqualified", dir.isAbsolute());
    String self = UserGroupInformation.getCurrentUser().getShortUserName();
    assertThat(dir.toString(),
        StringContains.containsString("/user/" + self + "/private"));
  }

}
