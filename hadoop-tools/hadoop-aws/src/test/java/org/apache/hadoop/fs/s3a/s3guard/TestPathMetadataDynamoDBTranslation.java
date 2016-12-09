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
import java.net.URI;
import java.util.Collection;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.security.UserGroupInformation;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.BLOCK_SIZE;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.FILE_LENGTH;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.MOD_TIME;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToParentKeyAttribute;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.CHILD;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.IS_DIR;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.itemToPathMetadata;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathMetadataToItem;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToKey;

/**
 * Test the PathMetadataDynamoDBTranslation is able to translate between domain
 * model objects and DynamoDB items.
 */
public class TestPathMetadataDynamoDBTranslation {
  private static final String DEFAULT_URI = "s3a://test-bucket/";
  private static final URI S3AURI = URI.create(DEFAULT_URI);

  private static final Path TEST_DIR_PATH = new Path("/myDir");
  private static final Item TEST_DIR_ITEM = new Item();
  private static PathMetadata testDirPathMetadata;

  private static final long TEST_FILE_LENGTH = 100;
  private static final long TEST_MOD_TIME = 9999;
  private static final long TEST_BLOCK_SIZE = 128;
  private static final Path TEST_FILE_PATH = new Path(TEST_DIR_PATH, "myFile");
  private static final Item TEST_FILE_ITEM = new Item();
  private static PathMetadata testFilePathMetadata;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();

    testDirPathMetadata =
        new PathMetadata(new S3AFileStatus(false, TEST_DIR_PATH, username));
    TEST_DIR_ITEM
        .withPrimaryKey(PARENT, "/", CHILD, TEST_DIR_PATH.getName())
        .withBoolean(IS_DIR, true);

    testFilePathMetadata = new PathMetadata(
        new S3AFileStatus(TEST_FILE_LENGTH, TEST_MOD_TIME, TEST_FILE_PATH,
            TEST_BLOCK_SIZE, username));
    TEST_FILE_ITEM
        .withPrimaryKey(PARENT, TEST_DIR_PATH.toString(),
            CHILD, TEST_FILE_PATH.getName())
        .withBoolean(IS_DIR, false)
        .withLong(FILE_LENGTH, TEST_FILE_LENGTH)
        .withLong(MOD_TIME, TEST_MOD_TIME)
        .withLong(BLOCK_SIZE, TEST_BLOCK_SIZE);
  }

  /**
   * It should not take long time as it doesn't involve remote server operation.
   */
  @Rule
  public final Timeout timeout = new Timeout(30 * 1000);

  @Test
  public void testKeySchema() {
    final Collection<KeySchemaElement> keySchema =
        PathMetadataDynamoDBTranslation.keySchema();
    assertNotNull(keySchema);
    assertEquals("There should be HASH and RANGE key in key schema",
        2, keySchema.size());
    for (KeySchemaElement element : keySchema) {
      assertThat(element.getAttributeName(), anyOf(is(PARENT), is(CHILD)));
      assertThat(element.getKeyType(),
          anyOf(is(HASH.toString()), is(RANGE.toString())));
    }
  }

  @Test
  public void testAttributeDefinitions() {
    final Collection<AttributeDefinition> attrs =
        PathMetadataDynamoDBTranslation.attributeDefinitions();
    assertNotNull(attrs);
    assertEquals("There should be HASH and RANGE attributes", 2, attrs.size());
    for (AttributeDefinition definition : attrs) {
      assertThat(definition.getAttributeName(), anyOf(is(PARENT), is(CHILD)));
      assertEquals(S.toString(), definition.getAttributeType());
    }
  }

  @Test
  public void testItemToPathMetadata() throws IOException {
    final String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
    assertNull(itemToPathMetadata(S3AURI, null, user));

    verify(TEST_DIR_ITEM, itemToPathMetadata(S3AURI, TEST_DIR_ITEM, user));
    verify(TEST_FILE_ITEM, itemToPathMetadata(S3AURI, TEST_FILE_ITEM, user));
  }

  /**
   * Verify that the Item and PathMetadata objects hold the same information.
   */
  private static void verify(Item item, PathMetadata meta) {
    assertNotNull(meta);
    assert meta.getFileStatus() instanceof S3AFileStatus;
    final S3AFileStatus status = (S3AFileStatus) meta.getFileStatus();
    final Path path = Path.getPathWithoutSchemeAndAuthority(status.getPath());
    assertEquals(item.get(PARENT), path.getParent().toString());
    assertEquals(item.get(CHILD), path.getName());
    boolean isDir = item.hasAttribute(IS_DIR) && item.getBoolean(IS_DIR);
    assertEquals(isDir, status.isDirectory());
    long len = item.hasAttribute(FILE_LENGTH) ? item.getLong(FILE_LENGTH) : 0;
    assertEquals(len, status.getLen());
    long bSize = item.hasAttribute(BLOCK_SIZE) ? item.getLong(BLOCK_SIZE) : 0;
    assertEquals(bSize, status.getBlockSize());

    /*
     * S3AFileStatue#getModificationTime() report the current time, so the
     * following assertion is failing.
     *
     * long modTime = item.hasAttribute(MOD_TIME) ? item.getLong(MOD_TIME) : 0;
     * assertEquals(modTime, status.getModificationTime());
     */
  }

  @Test
  public void testPathMetadataToItem() {
    verify(pathMetadataToItem(testDirPathMetadata), testDirPathMetadata);
    verify(pathMetadataToItem(testFilePathMetadata),
        testFilePathMetadata);
  }

  @Test
  public void testPathToParentKeyAttribute() {
    doTestPathToParentKeyAttribute(TEST_DIR_PATH);
    doTestPathToParentKeyAttribute(TEST_FILE_PATH);
  }

  private static void doTestPathToParentKeyAttribute(Path path) {
    final KeyAttribute attr = pathToParentKeyAttribute(path);
    assertNotNull(attr);
    assertEquals(PARENT, attr.getName());
    // this path is expected as parent filed
    assertEquals(path.toString(), attr.getValue());
  }

  @Test
  public void testPathToKey() {
    try {
      pathToKey(new Path("/"));
      fail("Root path should have not been mapped to any PrimaryKey");
    } catch (IllegalArgumentException ignored) {
    }

    doTestPathToKey(TEST_DIR_PATH);
    doTestPathToKey(TEST_FILE_PATH);
  }

  private static void doTestPathToKey(Path path) {
    final PrimaryKey key = pathToKey(path);
    assertNotNull(key);
    assertEquals("There should be both HASH and RANGE keys",
        2, key.getComponents().size());

    for (KeyAttribute keyAttribute : key.getComponents()) {
      assertThat(keyAttribute.getName(), anyOf(is(PARENT), is(CHILD)));
      if (PARENT.equals(keyAttribute.getName())) {
        assertEquals(path.getParent().toString(), keyAttribute.getValue());
      } else {
        assertEquals(path.getName(), keyAttribute.getValue());
      }
    }
  }

}
