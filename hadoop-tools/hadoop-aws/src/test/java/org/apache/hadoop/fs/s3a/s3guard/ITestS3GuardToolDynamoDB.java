/**
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

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.DestroyMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.InitMetadata;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test S3Guard related CLI commands against DynamoDB.
 */
public class ITestS3GuardToolDynamoDB extends S3GuardToolTestBase {

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
  public void testInitDynamoDBMetadataStore() throws IOException {
    final String testS3Url = getTestPath("init");
    String testTableName = "initDynamoDBMetadataStore" +
        new Random().nextInt();

    InitMetadata cmd = new InitMetadata(getFs().getConf());
    Table table = null;

    try {
      String[] args = new String[]{
          "init", "-m", "dynamodb://" + testTableName, testS3Url
      };
      assertEquals("Init command did not exit successfully - see output",
          SUCCESS, cmd.run(args));
      // Verify the existence of the dynamodb table.
      try {
        MetadataStore ms = getMetadataStore();
        assertTrue("metadata store should be DynamoDBMetadataStore",
            ms instanceof DynamoDBMetadataStore);
        DynamoDBMetadataStore dynamoMs = (DynamoDBMetadataStore) ms;
        DynamoDB db = dynamoMs.getDynamoDB();
        table = db.getTable(testTableName);
        table.describe();
      } catch (ResourceNotFoundException e) {
        fail(String.format("DynamoDB table %s does not exist",
            testTableName));
      }
    } finally {
      // Clean the table.
      try {
        if (table != null) {
          table.delete();
        }
      } catch (ResourceNotFoundException e) {
        // Ignore
      }
    }
  }

  @Test
  public void testDestroyDynamoDBMetadataStore()
      throws IOException, InterruptedException {
    final String testS3Url = getTestPath("destroy");
    String testTableName = "destroyDynamoDBMetadataStore" +
        new Random().nextInt();

    S3AFileSystem fs = getFs();
    DestroyMetadata cmd = new DestroyMetadata(fs.getConf());

    // Pre-alloc DynamoDB table.
    DynamoDB db = DynamoDBMetadataStore.createDynamoDB(fs);
    try {
      Table table = db.getTable(testTableName);
      table.delete();
      table.waitForDelete();
    } catch (ResourceNotFoundException e) {
      // Ignore.
    }
    Collection<KeySchemaElement> elements =
        PathMetadataDynamoDBTranslation.keySchema();
    Collection<AttributeDefinition> attrs =
        PathMetadataDynamoDBTranslation.attributeDefinitions();
    ProvisionedThroughput pt = new ProvisionedThroughput(100L, 200L);
    Table table = db.createTable(new CreateTableRequest()
        .withAttributeDefinitions(attrs)
        .withKeySchema(elements)
        .withTableName(testTableName)
        .withProvisionedThroughput(pt));
    try {
      table.waitForActive();
      assertTrue("Table does not exist", exist(db, testTableName));

      String[] args = new String[]{
          "destroy", "-m", "dynamodb://" + testTableName, testS3Url
      };
      assertEquals("Destroy command did not exit successfully - see output",
          SUCCESS, cmd.run(args));
      assertFalse(String.format("%s still exists", testTableName),
          exist(db, testTableName));
    } finally {
      if (table != null) {
        try {
          table.delete();
          table.waitForDelete();
        } catch (ResourceNotFoundException e) {}
      }
    }
  }
}
