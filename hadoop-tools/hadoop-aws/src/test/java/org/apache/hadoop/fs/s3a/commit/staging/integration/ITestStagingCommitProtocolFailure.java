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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_UPLOADS_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_NAME;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.S3A_COMMITTER_FACTORY_KEY;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verify that a staging committer cannot be created if the FS doesn't support multipart
 * uploads.
 */
public class ITestStagingCommitProtocolFailure extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBucketOverrides(getTestBucketName(conf), conf,
        S3A_COMMITTER_FACTORY_KEY,
        FS_S3A_COMMITTER_NAME,
        MULTIPART_UPLOADS_ENABLED);
    conf.setBoolean(MULTIPART_UPLOADS_ENABLED, false);
    conf.set(S3A_COMMITTER_FACTORY_KEY, CommitConstants.S3A_COMMITTER_FACTORY);
    conf.set(FS_S3A_COMMITTER_NAME, InternalCommitterConstants.COMMITTER_NAME_STAGING);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Test
  public void testCreateCommitter() throws Exception {
    TaskAttemptContext tContext = new TaskAttemptContextImpl(getConfiguration(),
        new TaskAttemptID());
    Path commitPath = methodPath();
    LOG.debug("Trying to create a committer on the path: {}", commitPath);
    intercept(PathCommitException.class,
        () -> new StagingCommitter(commitPath, tContext));
  }
}
