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

package org.apache.hadoop.fs.s3a.s3guard;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;

/**
 * This test run against the root of the FS, and operations which span the DDB
 * table.
 * For this reason, these tests are executed in the sequential phase of the
 * integration tests.
 * <p>
 * The tests only run if DynamoDB is the metastore.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestS3GuardRootOperations extends AbstractS3ATestBase {

  private StoreContext storeContext;

  private String fsUriStr;

  private DynamoDBMetadataStore metastore;

  private String metastoreUriStr;

  /**
   * The test timeout is increased in case previous tests have created
   * many tombstone markers which now need to be purged.
   * @return the test timeout.
   */
  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_SECONDS * 1000;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    // set a sleep time of 0 on pruning, for speedier test runs.
    removeBucketOverrides(bucketName, conf, ENABLE_MULTI_DELETE);
    conf.setTimeDuration(
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        0,
        TimeUnit.MILLISECONDS);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    S3ATestUtils.assumeS3GuardState(true, conf);
    storeContext = fs.createStoreContext();
    assume("Filesystem isn't running DDB",
        storeContext.getMetadataStore() instanceof DynamoDBMetadataStore);
    metastore = (DynamoDBMetadataStore) storeContext.getMetadataStore();
    URI fsURI = storeContext.getFsURI();
    fsUriStr = fsURI.toString();
    if (!fsUriStr.endsWith("/")) {
      fsUriStr = fsUriStr + "/";
    }
    metastoreUriStr = "dynamodb://" + metastore.getTableName() + "/";
  }

  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    super.teardown();
  }

  @Test
  public void test_100_FilesystemPrune() throws Throwable {
    describe("Execute prune against a filesystem URI");
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    int result = S3GuardTool.run(conf,
        S3GuardTool.Prune.NAME,
        fsUriStr);
    Assertions.assertThat(result)
        .describedAs("Result of prune %s", fsUriStr)
        .isEqualTo(0);
  }


  @Test
  public void test_200_MetastorePrune() throws Throwable {
    describe("Execute prune against a dynamo URL");
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    S3GuardTool.Prune cmd = new S3GuardTool.Prune(conf);
    int result = S3GuardTool.run(conf,
        S3GuardTool.Prune.NAME,
        "-meta", checkNotNull(metastoreUriStr),
        "-seconds", "1");
    Assertions.assertThat(result)
        .describedAs("Result of prune %s", fsUriStr)
        .isEqualTo(0);
  }

}
