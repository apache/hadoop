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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.Tag;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.UnknownStoreException;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Destroy;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Init;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_NAME_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_TAG;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3AUtils.setBucketOption;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.*;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.*;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test S3Guard related CLI commands against DynamoDB.
 */
public class ITestS3GuardToolDynamoDB extends AbstractS3GuardToolTestBase {

  @Override
  public void setup() throws Exception {
    super.setup();
    try {
      getMetadataStore();
    } catch (ClassCastException e) {
      throw new AssumptionViolatedException(
          "Test only applies when DynamoDB is used for S3Guard Store",
          e);
    }
  }

  @Override
  protected DynamoDBMetadataStore getMetadataStore() {
    return (DynamoDBMetadataStore) super.getMetadataStore();
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
    final String testTableName =
        getTestTableName("testInvalidRegion" + new Random().nextInt());
    final String testRegion = "invalidRegion";
    // Initialize MetadataStore
    final Init initCmd = toClose(new Init(getFileSystem().getConf()));
    intercept(IOException.class,
        () -> {
          int res = initCmd.run(new String[]{
              "init",
              "-region", testRegion,
              "-meta", "dynamodb://" + testTableName
          });
          return "Use of invalid region did not fail, returning " + res
              + "- table may have been " +
              "created and not cleaned up: " + testTableName;
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
        getTestTableName("testDynamoTableTagging-" + UUID.randomUUID()));
    String bucket = getFileSystem().getBucket();
    removeBucketOverrides(bucket, conf,
        S3GUARD_DDB_TABLE_NAME_KEY,
        S3GUARD_DDB_REGION_KEY);

    S3GuardTool.Init cmdR = new S3GuardTool.Init(conf);
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("hello", "dynamo");
    tagMap.put("tag", "youre it");

    String[] argsR = new String[]{
        cmdR.getName(),
        "-tag", tagMapToStringParams(tagMap),
        "s3a://" + bucket + "/"
    };

    // run
    cmdR.run(argsR);

    // Check. Should create new metadatastore with the table name set.
    try (DynamoDBMetadataStore ddbms = new DynamoDBMetadataStore()) {
      ddbms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
      ListTagsOfResourceRequest listTagsOfResourceRequest = new ListTagsOfResourceRequest()
          .withResourceArn(ddbms.getTable().getDescription().getTableArn());
      List<Tag> tags = ddbms.getAmazonDynamoDB().listTagsOfResource(listTagsOfResourceRequest).getTags();

      // assert
      // table version is always there as a plus one tag.
      assertEquals(tagMap.size() + 1, tags.size());
      for (Tag tag : tags) {
        // skip the version marker tag
        if (tag.getKey().equals(VERSION_MARKER_TAG_NAME)) {
          continue;
        }
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

  private DDBCapacities getCapacities() throws IOException {
    return DDBCapacities.extractCapacities(getMetadataStore().getDiagnostics());
  }

  @Test
  public void testDynamoDBInitDestroyCycle() throws Throwable {
    String testTableName =
        getTestTableName("testDynamoDBInitDestroy" + new Random().nextInt());
    String testS3Url = path(testTableName).toString();
    S3AFileSystem fs = getFileSystem();
    DynamoDB db = null;
    try {
      try (Init initCmd = new Init(fs.getConf())) {
      // Initialize MetadataStore
        expectSuccess("Init command did not exit successfully - see output",
            initCmd,
            Init.NAME,
            "-" + READ_FLAG, "0",
            "-" + WRITE_FLAG, "0",
            "-" + Init.SSE_FLAG,
            "-" + META_FLAG, "dynamodb://" + testTableName,
            testS3Url);
      }
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
      setBucketOption(conf, bucket, Constants.S3_METADATA_STORE_IMPL,
          Constants.S3GUARD_METASTORE_DYNAMO);
      try (Init initCmd = new Init(conf)) {
        String initOutput = exec(initCmd,
            "init", "-meta", "dynamodb://" + testTableName, testS3Url);
        assertTrue("No Dynamo diagnostics in output " + initOutput,
            initOutput.contains(DESCRIPTION));
      }

      // run a bucket info command and look for
      // confirmation that it got the output from DDB diags
      String info;
      try (S3GuardTool.BucketInfo infocmd = new S3GuardTool.BucketInfo(conf)) {
        info = exec(infocmd, BucketInfo.NAME,
            "-" + BucketInfo.GUARDED_FLAG,
            testS3Url);
        assertTrue("No Dynamo diagnostics in output " + info,
            info.contains(DESCRIPTION));
      }

    // get the current values to set again

      // play with the set-capacity option
      String fsURI = getFileSystem().getUri().toString();
      DDBCapacities original = getCapacities();
      assertTrue("Wrong billing mode in " + info,
          info.contains(BILLING_MODE_PER_REQUEST));
      // per-request tables fail here, so expect that
      intercept(IOException.class, E_ON_DEMAND_NO_SET_CAPACITY,
          () -> exec(toClose(newSetCapacity()),
              SetCapacity.NAME,
              fsURI));

         // Destroy MetadataStore
      try (Destroy destroyCmd = new Destroy(fs.getConf())){
        String destroyed = exec(destroyCmd,
            "destroy", "-meta", "dynamodb://" + testTableName, testS3Url);
        // Verify it does not exist
        assertFalse(String.format("%s still exists", testTableName),
            exist(db, testTableName));

        // delete again and expect success again
        expectSuccess("Destroy command did not exit successfully - see output",
            destroyCmd,
            "destroy", "-meta", "dynamodb://" + testTableName, testS3Url);
      }
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
          } catch (ResourceNotFoundException | ResourceInUseException e) {
            /* Ignore */
          }
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
        "-meta", "dynamodb://" + getTestTableName(DYNAMODB_TABLE));
  }

  @Test
  public void testCLIFsckWithoutParam() throws Exception {
    intercept(ExitUtil.ExitException.class, () -> run(Fsck.NAME));
  }

  @Test
  public void testCLIFsckWithParam() throws Exception {
    LOG.info("This test serves the purpose to run fsck with the correct " +
        "parameters, so there will be no exception thrown.");
    final int result = run(S3GuardTool.Fsck.NAME, "-check",
        "s3a://" + getFileSystem().getBucket());
    LOG.info("The return value of the run: {}", result);
  }

  @Test
  public void testCLIFsckWithParamParentOfRoot() throws Exception {
    intercept(IOException.class, "Invalid URI",
        () -> run(S3GuardTool.Fsck.NAME, "-check",
            "s3a://" + getFileSystem().getBucket() + "/.."));
  }

  @Test
  public void testCLIFsckFailInitializeFs() throws Exception {
    intercept(UnknownStoreException.class,
        () -> run(S3GuardTool.Fsck.NAME, "-check",
            "s3a://this-bucket-does-not-exist-" + UUID.randomUUID()));
  }

  @Test
  public void testCLIFsckDDbInternalWrongS3APath() throws Exception {
    intercept(FileNotFoundException.class, "wrong path",
        () -> run(S3GuardTool.Fsck.NAME, "-"+Fsck.DDB_MS_CONSISTENCY_FLAG,
            "s3a://" + getFileSystem().getBucket() + "/" + UUID.randomUUID()));
  }

  @Test
  public void testCLIFsckDDbInternalParam() throws Exception {
    describe("This test serves the purpose to run fsck with the correct " +
        "parameters, so there will be no exception thrown.");
    final int result = run(S3GuardTool.Fsck.NAME,
        "-" + Fsck.DDB_MS_CONSISTENCY_FLAG,
        "s3a://" + getFileSystem().getBucket());
    LOG.info("The return value of the run: {}", result);
  }

  @Test
  public void testCLIFsckCheckExclusive() throws Exception {
    describe("There should be only one check param when running fsck." +
        "If more then one param is passed, the command should fail." +
        "This provide exclusive run for checks so the user is able to define " +
        "the order of checking.");
    intercept(ExitUtil.ExitException.class, "only one parameter",
        () -> run(S3GuardTool.Fsck.NAME,
        "-" + Fsck.DDB_MS_CONSISTENCY_FLAG, "-" + Fsck.CHECK_FLAG,
        "s3a://" + getFileSystem().getBucket()));
  }

  @Test
  public void testCLIFsckDDbFixOnlyFails() throws Exception {
    describe("This test serves the purpose to run fsck with the correct " +
        "parameters, so there will be no exception thrown.");
    final int result = run(S3GuardTool.Fsck.NAME,
        "-" + Fsck.FIX_FLAG,
        "s3a://" + getFileSystem().getBucket());
    LOG.info("The return value of the run: {}", result);
    assertEquals(ERROR, result);
  }

  /**
   * Test that the fix flag is accepted by the fsck.
   *
   * Note that we don't have an assert at the end of this test because
   * there maybe some errors found during the check and the returned value
   * will be ERROR and not SUCCESS. So if we assert on SUCCESS, then the test
   * could (likely) to be flaky.
   * If the FIX_FLAG parameter is not accepted here an exception will be thrown
   * so the test will break.
   *
   * @throws Exception
   */
  @Test
  public void testCLIFsckDDbFixAndInternalSucceed() throws Exception {
    describe("This test serves the purpose to run fsck with the correct " +
        "parameters, so there will be no exception thrown.");
    final int result = run(S3GuardTool.Fsck.NAME,
        "-" + Fsck.FIX_FLAG,
        "-" + Fsck.DDB_MS_CONSISTENCY_FLAG,
        "s3a://" + getFileSystem().getBucket());
    LOG.info("The return value of the run: {}", result);
  }

  /**
   * Test that when init, the CMK option can not live without SSE enabled.
   */
  @Test
  public void testCLIInitParamCmkWithoutSse() throws Exception {
    intercept(ExitUtil.ExitException.class, "can only be used with",
        () -> run(S3GuardTool.Init.NAME,
            "-" + S3GuardTool.CMK_FLAG,
            "alias/" + UUID.randomUUID(),
            "s3a://" + getFileSystem().getBucket() + "/" + UUID.randomUUID()));
  }

}
