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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_READ_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY;

/**
 * Tests concurrent operations on S3Guard.
 */
public class ITestS3GuardConcurrentOps extends AbstractS3ATestBase {

  @Rule
  public final Timeout timeout = new Timeout(5 * 60 * 1000);

  protected Configuration createConfiguration() {
    Configuration conf =  super.createConfiguration();
    //patch the read/write capacity
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, 0);
    conf.setInt(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, 0);
    return conf;
  }

  private void failIfTableExists(DynamoDB db, String tableName) {
    boolean tableExists = true;
    try {
      Table table = db.getTable(tableName);
      table.describe();
    } catch (ResourceNotFoundException e) {
      tableExists = false;
    }
    if (tableExists) {
      fail("Table already exists: " + tableName);
    }
  }

  private void deleteTable(DynamoDB db, String tableName) throws
      InterruptedException {
    try {
      Table table = db.getTable(tableName);
      table.waitForActive();
      table.delete();
      table.waitForDelete();
    } catch (ResourceNotFoundException e) {
      LOG.warn("Failed to delete {}, as it was not found", tableName, e);
    }
  }

  @Test
  public void testConcurrentTableCreations() throws Exception {
    S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();
    Assume.assumeTrue("Test only applies when DynamoDB is used for S3Guard",
        conf.get(Constants.S3_METADATA_STORE_IMPL).equals(
            Constants.S3GUARD_METASTORE_DYNAMO));

    AWSCredentialProviderList sharedCreds =
        fs.shareCredentials("testConcurrentTableCreations");
    // close that shared copy.
    sharedCreds.close();
    // this is the original reference count.
    int originalRefCount = sharedCreds.getRefCount();

    //now init the store; this should increment the ref count.
    DynamoDBMetadataStore ms = new DynamoDBMetadataStore();
    ms.initialize(fs, new S3Guard.TtlTimeProvider(conf));

    // the ref count should have gone up
    assertEquals("Credential Ref count unchanged after initializing metastore "
        + sharedCreds,
        originalRefCount + 1, sharedCreds.getRefCount());
    try {
      DynamoDB db = ms.getDynamoDB();

      String tableName =
          getTestTableName("testConcurrentTableCreations" +
              new Random().nextInt());
      conf.setBoolean(Constants.S3GUARD_DDB_TABLE_CREATE_KEY, true);
      conf.set(Constants.S3GUARD_DDB_TABLE_NAME_KEY, tableName);

      String region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
      if (StringUtils.isEmpty(region)) {
        // no region set, so pick it up from the test bucket
        conf.set(S3GUARD_DDB_REGION_KEY, fs.getBucketLocation());
      }
      int concurrentOps = 16;
      int iterations = 4;

      failIfTableExists(db, tableName);

      for (int i = 0; i < iterations; i++) {
        ExecutorService executor = Executors.newFixedThreadPool(
            concurrentOps, new ThreadFactory() {
              private AtomicInteger count = new AtomicInteger(0);

              public Thread newThread(Runnable r) {
                return new Thread(r,
                    "testConcurrentTableCreations" + count.getAndIncrement());
              }
            });
        ((ThreadPoolExecutor) executor).prestartAllCoreThreads();
        Future<Exception>[] futures = new Future[concurrentOps];
        for (int f = 0; f < concurrentOps; f++) {
          final int index = f;
          futures[f] = executor.submit(new Callable<Exception>() {
            @Override
            public Exception call() throws Exception {

              ContractTestUtils.NanoTimer timer =
                  new ContractTestUtils.NanoTimer();

              Exception result = null;
              try (DynamoDBMetadataStore store = new DynamoDBMetadataStore()) {
                store.initialize(conf, new S3Guard.TtlTimeProvider(conf));
              } catch (Exception e) {
                LOG.error(e.getClass() + ": " + e.getMessage());
                result = e;
              }

              timer.end("Parallel DynamoDB client creation %d", index);
              LOG.info("Parallel DynamoDB client creation {} ran from {} to {}",
                  index, timer.getStartTime(), timer.getEndTime());
              return result;
            }
          });
        }
        List<Exception> exceptions = new ArrayList<>(concurrentOps);
        for (int f = 0; f < concurrentOps; f++) {
          Exception outcome = futures[f].get();
          if (outcome != null) {
            exceptions.add(outcome);
          }
        }
        deleteTable(db, tableName);
        int exceptionsThrown = exceptions.size();
        if (exceptionsThrown > 0) {
          // at least one exception was thrown. Fail the test & nest the first
          // exception caught
          throw new AssertionError(exceptionsThrown + "/" + concurrentOps +
              " threads threw exceptions while initializing on iteration " + i,
              exceptions.get(0));
        }
      }
    } finally {
      ms.close();
    }
    assertEquals("Credential Ref count unchanged after closing metastore: "
            + sharedCreds,
        originalRefCount, sharedCreds.getRefCount());
  }
}
