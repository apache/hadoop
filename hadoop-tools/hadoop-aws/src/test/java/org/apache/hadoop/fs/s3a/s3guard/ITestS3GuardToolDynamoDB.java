/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.Test;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Destroy;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Init;
import org.apache.hadoop.test.LambdaTestUtils;

/**
 * Test S3Guard related CLI commands against DynamoDB.
 */
public class ITestS3GuardToolDynamoDB extends AbstractS3GuardToolTestBase {

  @Override
  protected MetadataStore newMetadataStore() {
    return new DynamoDBMetadataStore();
  }

  // Check the existence of a given DynamoDB table.
  private static boolean exist(DynamoDB dynamoDB, String tableName) {
    assertNotNull(dynamoDB);
    assertNotNull(tableName);
    assertFalse("empty table name", tableName.isEmpty());
    try {
      Table table = dynamoDB.getTable(tableName);
      table.describe();
    } catch (ResourceNotFoundException e) {
      return false;
    }
    return true;
  }

  @Test
  public void testInvalidRegion() throws Exception {
    final String testTableName = "testInvalidRegion" + new Random().nextInt();
    final String testRegion = "invalidRegion";
    // Initialize MetadataStore
    final Init initCmd = new Init(getFileSystem().getConf());
    LambdaTestUtils.intercept(IOException.class,
        new Callable<String>() {
          @Override
          public String call() throws Exception {
            int res = initCmd.run(new String[]{
                "init",
                "-region", testRegion,
                "-meta", "dynamodb://" + testTableName
            });
            return "Use of invalid region did not fail, returning " + res
                + "- table may have been " +
                "created and not cleaned up: " + testTableName;
          }
        });
  }

  @Test
  public void testDynamoDBInitDestroyCycle() throws Exception {
    String testTableName = "testDynamoDBInitDestroy" + new Random().nextInt();
    String testS3Url = path(testTableName).toString();
    S3AFileSystem fs = getFileSystem();
    DynamoDB db = null;
    try {
      // Initialize MetadataStore
      Init initCmd = new Init(fs.getConf());
      expectSuccess("Init command did not exit successfully - see output",
          initCmd,
          "init", "-meta", "dynamodb://" + testTableName, testS3Url);
      // Verify it exists
      MetadataStore ms = getMetadataStore();
      assertTrue("metadata store should be DynamoDBMetadataStore",
          ms instanceof DynamoDBMetadataStore);
      DynamoDBMetadataStore dynamoMs = (DynamoDBMetadataStore) ms;
      db = dynamoMs.getDynamoDB();
      assertTrue(String.format("%s does not exist", testTableName),
          exist(db, testTableName));

      // Destroy MetadataStore
      Destroy destroyCmd = new Destroy(fs.getConf());

      expectSuccess("Destroy command did not exit successfully - see output",
          destroyCmd,
          "destroy", "-meta", "dynamodb://" + testTableName, testS3Url);
      // Verify it does not exist
      assertFalse(String.format("%s still exists", testTableName),
          exist(db, testTableName));

      // delete again and expect success again
      expectSuccess("Destroy command did not exit successfully - see output",
          destroyCmd,
          "destroy", "-meta", "dynamodb://" + testTableName, testS3Url);
    } catch (ResourceNotFoundException e) {
      throw new AssertionError(
          String.format("DynamoDB table %s does not exist", testTableName),
          e);
    } finally {
      LOG.warn("Table may have not been cleaned up: " +
          testTableName);
      if (db != null) {
        Table table = db.getTable(testTableName);
        if (table != null) {
          try {
            table.delete();
            table.waitForDelete();
          } catch (ResourceNotFoundException e) { /* Ignore */ }
        }
      }
    }
  }
}
