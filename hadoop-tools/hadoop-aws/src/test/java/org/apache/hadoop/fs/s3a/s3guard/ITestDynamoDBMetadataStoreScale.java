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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.scale.AbstractITestS3AMetadataStoreScale;

import static org.apache.hadoop.fs.s3a.s3guard.MetadataStoreTestBase.basicFileStatus;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.junit.Assume.*;

/**
 * Scale test for DynamoDBMetadataStore.
 */
public class ITestDynamoDBMetadataStoreScale
    extends AbstractITestS3AMetadataStoreScale {

  private static final long BATCH_SIZE = 25;
  private static final long SMALL_IO_UNITS = BATCH_SIZE / 4;

  @Override
  public MetadataStore createMetadataStore() throws IOException {
    Configuration conf = getFileSystem().getConf();
    String ddbTable = conf.get(S3GUARD_DDB_TABLE_NAME_KEY);
    assumeNotNull("DynamoDB table is configured", ddbTable);
    String ddbEndpoint = conf.get(S3GUARD_DDB_REGION_KEY);
    assumeNotNull("DynamoDB endpoint is configured", ddbEndpoint);

    DynamoDBMetadataStore ms = new DynamoDBMetadataStore();
    ms.initialize(getFileSystem().getConf());
    return ms;
  }


  /**
   * Though the AWS SDK claims in documentation to handle retries and
   * exponential backoff, we have witnessed
   * com.amazonaws...dynamodbv2.model.ProvisionedThroughputExceededException
   * (Status Code: 400; Error Code: ProvisionedThroughputExceededException)
   * Hypothesis:
   * Happens when the size of a batched write is bigger than the number of
   * provisioned write units.  This test ensures we handle the case
   * correctly, retrying w/ smaller batch instead of surfacing exceptions.
   */
  @Test
  public void testBatchedWriteExceedsProvisioned() throws Exception {

    final long iterations = 5;
    boolean isProvisionedChanged;
    List<PathMetadata> toCleanup = new ArrayList<>();

    // Fail if someone changes a constant we depend on
    assertTrue("Maximum batch size must big enough to run this test",
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT >= BATCH_SIZE);

    try (DynamoDBMetadataStore ddbms =
         (DynamoDBMetadataStore)createMetadataStore()) {

      DynamoDB ddb = ddbms.getDynamoDB();
      String tableName = ddbms.getTable().getTableName();
      final ProvisionedThroughputDescription existing =
          ddb.getTable(tableName).describe().getProvisionedThroughput();

      // If you set the same provisioned I/O as already set it throws an
      // exception, avoid that.
      isProvisionedChanged = (existing.getReadCapacityUnits() != SMALL_IO_UNITS
          || existing.getWriteCapacityUnits() != SMALL_IO_UNITS);

      if (isProvisionedChanged) {
        // Set low provisioned I/O for dynamodb
        describe("Provisioning dynamo tbl %s read/write -> %d/%d", tableName,
            SMALL_IO_UNITS, SMALL_IO_UNITS);
        // Blocks to ensure table is back to ready state before we proceed
        ddbms.provisionTableBlocking(SMALL_IO_UNITS, SMALL_IO_UNITS);
      } else {
        describe("Skipping provisioning table I/O, already %d/%d",
            SMALL_IO_UNITS, SMALL_IO_UNITS);
      }

      try {
        // We know the dynamodb metadata store will expand a put of a path
        // of depth N into a batch of N writes (all ancestors are written
        // separately up to the root).  (Ab)use this for an easy way to write
        // a batch of stuff that is bigger than the provisioned write units
        try {
          describe("Running %d iterations of batched put, size %d", iterations,
              BATCH_SIZE);
          long pruneItems = 0;
          for (long i = 0; i < iterations; i++) {
            Path longPath = pathOfDepth(BATCH_SIZE, String.valueOf(i));
            FileStatus status = basicFileStatus(longPath, 0, false, 12345,
                12345);
            PathMetadata pm = new PathMetadata(status);

            ddbms.put(pm);
            toCleanup.add(pm);
            pruneItems++;
            // Having hard time reproducing Exceeded exception with put, also
            // try occasional prune, which was the only stack trace I've seen
            // (on JIRA)
            if (pruneItems == BATCH_SIZE) {
              describe("pruning files");
              ddbms.prune(Long.MAX_VALUE /* all files */);
              pruneItems = 0;
            }
          }
        } finally {
          describe("Cleaning up table %s", tableName);
          for (PathMetadata pm : toCleanup) {
            cleanupMetadata(ddbms, pm);
          }
        }
      } finally {
        if (isProvisionedChanged) {
          long write = existing.getWriteCapacityUnits();
          long read = existing.getReadCapacityUnits();
          describe("Restoring dynamo tbl %s read/write -> %d/%d", tableName,
              read, write);
          ddbms.provisionTableBlocking(existing.getReadCapacityUnits(),
              existing.getWriteCapacityUnits());
        }
      }
    }
  }

  // Attempt do delete metadata, suppressing any errors
  private void cleanupMetadata(MetadataStore ms, PathMetadata pm) {
    try {
      ms.forgetMetadata(pm.getFileStatus().getPath());
    } catch (IOException ioe) {
      // Ignore.
    }
  }

  private Path pathOfDepth(long n, @Nullable String fileSuffix) {
    StringBuilder sb = new StringBuilder();
    for (long i = 0; i < n; i++) {
      sb.append(i == 0 ? "/" + this.getClass().getSimpleName() : "lvl");
      sb.append(i);
      if (i == n-1 && fileSuffix != null) {
        sb.append(fileSuffix);
      }
      sb.append("/");
    }
    return new Path(getFileSystem().getUri().toString(), sb.toString());
  }
}
