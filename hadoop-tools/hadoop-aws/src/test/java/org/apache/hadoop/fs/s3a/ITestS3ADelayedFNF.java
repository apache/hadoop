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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Assume;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_INTERVAL;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Tests behavior of a FileNotFound error that happens after open(), i.e. on
 * the first read.
 */
public class ITestS3ADelayedFNF extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // reduce retry limit so FileNotFoundException cases timeout faster,
    // speeding up the tests
    removeBaseAndBucketOverrides(conf,
        CHANGE_DETECT_SOURCE,
        CHANGE_DETECT_MODE,
        RETRY_LIMIT,
        RETRY_INTERVAL,
        METADATASTORE_AUTHORITATIVE);
    conf.setInt(RETRY_LIMIT, 2);
    conf.set(RETRY_INTERVAL, "1ms");
    return conf;
  }

  /**
   * See debugging documentation
   * <a href="https://cwiki.apache.org/confluence/display/HADOOP/S3A%3A+FileNotFound+Exception+on+Read">here</a>.
   * @throws Exception
   */
  @Test
  public void testNotFoundFirstRead() throws Exception {
    S3AFileSystem fs = getFileSystem();
    ChangeDetectionPolicy changeDetectionPolicy =
        fs.getChangeDetectionPolicy();
    Assume.assumeFalse("FNF not expected when using a bucket with"
            + " object versioning",
        changeDetectionPolicy.getSource() == Source.VersionId);

    Path p = path("some-file");
    ContractTestUtils.createFile(fs, p, false, new byte[] {20, 21, 22});

    final FSDataInputStream in = fs.open(p);
    assertDeleted(p, false);

    // This should fail since we deleted after the open.
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> in.read());
  }

}
