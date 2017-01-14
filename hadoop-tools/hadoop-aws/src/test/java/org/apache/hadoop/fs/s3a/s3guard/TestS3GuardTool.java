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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.DestroyMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Diff;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.InitMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test S3Guard related CLI commands.
 */
public class TestS3GuardTool {

  private static final String OWNER = "hdfs";

  private Configuration conf;
  private MetadataStore ms;
  private S3AFileSystem fs;

  /** Get test path of s3. */
  private String getTestPath(String path) {
    return fs.qualify(new Path(path)).toString();
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = S3ATestUtils.createTestFileSystem(conf);

    ms = new LocalMetadataStore();
    ms.initialize(fs);
  }

  @After
  public void tearDown() {
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
    final String testTableName = "s3guard_test_init_ddb_table";
    InitMetadata cmd = new InitMetadata(fs.getConf());
    Table table = null;

    try {
      String[] args = new String[]{
          "init", "-m", "dynamodb://" + testTableName,
      };
      assertEquals(SUCCESS, cmd.run(args));
      // Verify the existence of the dynamodb table.
      try {
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
    final String testTableName = "s3guard_test_destroy_ddb_table";
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
    table.waitForActive();
    assertTrue(exist(db, testTableName));

    String[] args = new String[]{
        "destroy", "-m", "dynamodb://" + testTableName,
    };
    assertEquals(SUCCESS, cmd.run(args));
    assertFalse(String.format("%s still exists", testTableName),
        exist(db, testTableName));
  }

  @Test
  public void testImportCommand() throws IOException {
    fs.mkdirs(new Path("/test"));
    Path dir = new Path("/test/a");
    fs.mkdirs(dir);
    for (int i = 0; i < 10; i++) {
      String child = String.format("file-%d", i);
      try (FSDataOutputStream out = fs.create(new Path(dir, child))) {
        out.write(1);
      }
    }

    S3GuardTool.Import cmd = new S3GuardTool.Import(fs.getConf());
    cmd.setMetadataStore(ms);

    assertEquals(0, cmd.run(new String[]{"import", getTestPath("/test/a")}));

    DirListingMetadata children =
        ms.listChildren(new Path(getTestPath("/test/a")));
    assertEquals(10, children.getListing().size());
    // assertTrue(children.isAuthoritative());
  }

  private void mkdirs(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      fs.mkdirs(path);
    }
    if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(true, path, OWNER);
      ms.put(new PathMetadata(status));
    }
  }

  private static void putFile(MetadataStore ms, S3AFileStatus f)
      throws IOException {
    assertNotNull(f);
    ms.put(new PathMetadata(f));
    Path parent = f.getPath().getParent();
    while (parent != null) {
      S3AFileStatus dir = new S3AFileStatus(false, parent, f.getOwner());
      ms.put(new PathMetadata(dir));
      parent = parent.getParent();
    }
  }

  /**
   * Create file either on S3 or in metadata store.
   * @param path the file path.
   * @param onS3 set to true to create the file on S3.
   * @param onMetadataStore set to true to create the file on the
   *                        metadata store.
   * @throws IOException
   */
  private void createFile(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      ContractTestUtils.touch(fs, path);
    }

    if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(100L, 10000L,
          fs.qualify(path), 512L, "hdfs");
      putFile(ms, status);
    }
  }

  @Test
  public void testDiffCommand() throws IOException {
    Set<Path> filesOnS3 = new HashSet<>();  // files on S3.
    Set<Path> filesOnMS = new HashSet<>();  // files on metadata store.

    String testPath = getTestPath("/test-diff");
    mkdirs(new Path(testPath), true, true);

    Path msOnlyPath = new Path(testPath, "ms_only");
    mkdirs(msOnlyPath, false, true);
    filesOnMS.add(msOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(msOnlyPath, String.format("file-%d", i));
      createFile(file, false, true);
      filesOnMS.add(file);
    }

    Path s3OnlyPath = new Path(testPath, "s3_only");
    mkdirs(s3OnlyPath, true, false);
    filesOnS3.add(s3OnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(s3OnlyPath, String.format("file-%d", i));
      createFile(file, true, false);
      filesOnS3.add(file);
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf);
    Diff cmd = new Diff(fs.getConf());
    cmd.setMetadataStore(ms);
    assertEquals(SUCCESS,
        cmd.run(new String[]{"diff", "-m", "local://metadata", testPath}, out));

    Set<Path> actualOnS3 = new HashSet<>();
    Set<Path> actualOnMS = new HashSet<>();
    try (ByteArrayInputStream in =
             new ByteArrayInputStream(buf.toByteArray())) {
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(in))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\s");
          assertEquals("[" + line + "] does not have enough fields",
              3, fields.length);
          String where = fields[0];
          if (Diff.S3_PREFIX.equals(where)) {
            actualOnS3.add(new Path(fields[2]));
          } else if (Diff.MS_PREFIX.equals(where)) {
            actualOnMS.add(new Path(fields[2]));
          } else {
            fail("Unknown prefix: " + where);
          }
        }
      }
    }
    String actualOut = out.toString();
    assertEquals("Mismatched metadata store outputs: " + actualOut,
        filesOnMS, actualOnMS);
    assertEquals("Mismatched s3 outputs: " + actualOut, filesOnS3, actualOnS3);
  }
}
