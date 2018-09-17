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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.Tag;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Destroy;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Init;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_NAME_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_TAG;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.*;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test S3Guard related CLI commands against DynamoDB.
 */
public class ITestS3GuardToolDynamoDB extends AbstractS3GuardToolTestBase {

  @Override
  public void setup() throws Exception {
    super.setup();
    MetadataStore ms = getMetadataStore();
    Assume.assumeTrue("Test only applies when DynamoDB is used for S3Guard;"
        + "Store is " + (ms == null ? "none" : ms.toString()),
        ms instanceof DynamoDBMetadataStore);
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
  public void testDynamoTableTagging() throws Exception {
    Configuration conf = getConfiguration();
    // If the region is not set in conf, skip the test.
    String ddbRegion = conf.get(S3GUARD_DDB_REGION_KEY);
    Assume.assumeTrue(
        S3GUARD_DDB_REGION_KEY + " should be set to run this test",
        ddbRegion != null && !ddbRegion.isEmpty()
    );

    // setup
    // clear all table tagging config before this test
    conf.getPropsWithPrefix(S3GUARD_DDB_TABLE_TAG).keySet().forEach(
        propKey -> conf.unset(S3GUARD_DDB_TABLE_TAG + propKey)
    );

    conf.set(S3GUARD_DDB_TABLE_NAME_KEY,
        "testDynamoTableTagging-" + UUID.randomUUID());
    S3GuardTool.Init cmdR = new S3GuardTool.Init(conf);
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("hello", "dynamo");
    tagMap.put("tag", "youre it");

    String[] argsR = new String[]{
        cmdR.getName(),
        "-tag", tagMapToStringParams(tagMap)
    };

    // run
    cmdR.run(argsR);

    // Check. Should create new metadatastore with the table name set.
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf);
      ListTagsOfResourceRequest listTagsOfResourceRequest = new ListTagsOfResourceRequest()
          .withResourceArn(ddbms.getTable().getDescription().getTableArn());
      List<Tag> tags = ddbms.getAmazonDynamoDB().listTagsOfResource(listTagsOfResourceRequest).getTags();

      // assert
      assertEquals(tagMap.size(), tags.size());
      for (Tag tag : tags) {
        Assert.assertEquals(tagMap.get(tag.getKey()), tag.getValue());
      }
      // be sure to clean up - delete table
      ddbms.destroy();
    }
  }

  private String tagMapToStringParams(Map<String, String> tagMap) {
    StringBuilder stringBuilder = new StringBuilder();

    for (Map.Entry<String, String> kv : tagMap.entrySet()) {
      stringBuilder.append(kv.getKey() + "=" + kv.getValue() + ";");
    }

    return stringBuilder.toString();
  }


  private static class Capacities {
    private final long read, write;

    Capacities(long read, long write) {
      this.read = read;
      this.write = write;
    }

    public long getRead() {
      return read;
    }

    public long getWrite() {
      return write;
    }

    String getReadStr() {
      return Long.toString(read);
    }

    String getWriteStr() {
      return Long.toString(write);
    }

    void checkEquals(String text, Capacities that) throws Exception {
      if (!this.equals(that)) {
        throw new Exception(text + " expected = " + this +"; actual = "+ that);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Capacities that = (Capacities) o;
      return read == that.read && write == that.write;
    }

    @Override
    public int hashCode() {
      return Objects.hash(read, write);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Capacities{");
      sb.append("read=").append(read);
      sb.append(", write=").append(write);
      sb.append('}');
      return sb.toString();
    }
  }

  private Capacities getCapacities() throws IOException {
    Map<String, String> diagnostics = getMetadataStore().getDiagnostics();
    return getCapacities(diagnostics);
  }

  private Capacities getCapacities(Map<String, String> diagnostics) {
    return new Capacities(
        Long.parseLong(diagnostics.get(DynamoDBMetadataStore.READ_CAPACITY)),
        Long.parseLong(diagnostics.get(DynamoDBMetadataStore.WRITE_CAPACITY)));
  }

  @Test
  public void testDynamoDBInitDestroyCycle() throws Throwable {
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

      Configuration conf = fs.getConf();
      String bucket = fs.getBucket();
      // force in a new bucket
      S3AUtils.setBucketOption(conf, bucket, Constants.S3_METADATA_STORE_IMPL,
          Constants.S3GUARD_METASTORE_DYNAMO);
      initCmd = new Init(conf);
      String initOutput = exec(initCmd,
          "init", "-meta", "dynamodb://" + testTableName, testS3Url);
      assertTrue("No Dynamo diagnostics in output " + initOutput,
          initOutput.contains(DESCRIPTION));

      // run a bucket info command and look for
      // confirmation that it got the output from DDB diags
      S3GuardTool.BucketInfo infocmd = new S3GuardTool.BucketInfo(conf);
      String info = exec(infocmd, S3GuardTool.BucketInfo.NAME,
          "-" + S3GuardTool.BucketInfo.GUARDED_FLAG,
          testS3Url);
      assertTrue("No Dynamo diagnostics in output " + info,
          info.contains(DESCRIPTION));

      // get the current values to set again

      // play with the set-capacity option
      Capacities original = getCapacities();
      String fsURI = getFileSystem().getUri().toString();
      String capacityOut = exec(newSetCapacity(),
          S3GuardTool.SetCapacity.NAME,
          fsURI);
      LOG.info("Set Capacity output=\n{}", capacityOut);
      capacityOut = exec(newSetCapacity(),
          S3GuardTool.SetCapacity.NAME,
          "-" + READ_FLAG, original.getReadStr(),
          "-" + WRITE_FLAG, original.getWriteStr(),
          fsURI);
      LOG.info("Set Capacity output=\n{}", capacityOut);

      // that call does not change the values
      original.checkEquals("unchanged", getCapacities());

         // Destroy MetadataStore
      Destroy destroyCmd = new Destroy(fs.getConf());

      String destroyed = exec(destroyCmd,
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

  private S3GuardTool newSetCapacity() {
    S3GuardTool setCapacity = new S3GuardTool.SetCapacity(
        getFileSystem().getConf());
    setCapacity.setStore(getMetadataStore());
    return setCapacity;
  }

  @Test
  public void testDestroyUnknownTable() throws Throwable {
    run(S3GuardTool.Destroy.NAME,
        "-region", "us-west-2",
        "-meta", DYNAMODB_TABLE);
  }

}
