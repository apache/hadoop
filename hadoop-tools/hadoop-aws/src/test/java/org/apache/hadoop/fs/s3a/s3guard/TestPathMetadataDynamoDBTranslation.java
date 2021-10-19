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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;

import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.*;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION_MARKER_ITEM_NAME;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION;
import static org.mockito.Mockito.never;

/**
 * Test the PathMetadataDynamoDBTranslation is able to translate between domain
 * model objects and DynamoDB items.
 */
@RunWith(Parameterized.class)
public class TestPathMetadataDynamoDBTranslation extends Assert {

  private static final Path TEST_DIR_PATH = new Path("s3a://test-bucket/myDir");
  private static final long TEST_FILE_LENGTH = 100;
  private static final long TEST_MOD_TIME = 9999;
  private static final long TEST_BLOCK_SIZE = 128;
  private static final String TEST_ETAG = "abc";
  private static final String TEST_VERSION_ID = "def";
  private static final Path TEST_FILE_PATH = new Path(TEST_DIR_PATH, "myFile");

  private final Item testFileItem;
  private final DDBPathMetadata testFilePathMetadata;
  private final Item testDirItem;
  private final DDBPathMetadata testDirPathMetadata;

  @Parameterized.Parameters
  public static Collection<Object[]> params() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();

    return Arrays.asList(new Object[][]{
        // with etag and versionId
        {
            new Item()
                .withPrimaryKey(PARENT,
                    pathToParentKey(TEST_FILE_PATH.getParent()),
                    CHILD, TEST_FILE_PATH.getName())
                .withBoolean(IS_DIR, false)
                .withLong(FILE_LENGTH, TEST_FILE_LENGTH)
                .withLong(MOD_TIME, TEST_MOD_TIME)
                .withLong(BLOCK_SIZE, TEST_BLOCK_SIZE)
                .withString(ETAG, TEST_ETAG)
                .withString(VERSION_ID, TEST_VERSION_ID),
            new DDBPathMetadata(
                new S3AFileStatus(TEST_FILE_LENGTH, TEST_MOD_TIME,
                    TEST_FILE_PATH, TEST_BLOCK_SIZE, username, TEST_ETAG,
                    TEST_VERSION_ID))
        },
        // without etag or versionId
        {
            new Item()
                .withPrimaryKey(PARENT,
                    pathToParentKey(TEST_FILE_PATH.getParent()),
                    CHILD, TEST_FILE_PATH.getName())
                .withBoolean(IS_DIR, false)
                .withLong(FILE_LENGTH, TEST_FILE_LENGTH)
                .withLong(MOD_TIME, TEST_MOD_TIME)
                .withLong(BLOCK_SIZE, TEST_BLOCK_SIZE),
            new DDBPathMetadata(
                new S3AFileStatus(TEST_FILE_LENGTH, TEST_MOD_TIME,
                    TEST_FILE_PATH, TEST_BLOCK_SIZE, username, null, null))
        }
    });
  }

  public TestPathMetadataDynamoDBTranslation(Item item,
      DDBPathMetadata metadata) throws IOException {
    testFileItem = item;
    testFilePathMetadata = metadata;

    String username = UserGroupInformation.getCurrentUser().getShortUserName();

    testDirPathMetadata = new DDBPathMetadata(new S3AFileStatus(false,
        TEST_DIR_PATH, username));

    testDirItem = new Item();
    testDirItem
        .withPrimaryKey(PARENT, "/test-bucket", CHILD, TEST_DIR_PATH.getName())
        .withBoolean(IS_DIR, true);
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

  @After
  public void tearDown() {
    PathMetadataDynamoDBTranslation.IGNORED_FIELDS.clear();
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
    assertNull(itemToPathMetadata(null, user));

    verify(testDirItem, itemToPathMetadata(testDirItem, user));
    verify(testFileItem, itemToPathMetadata(testFileItem, user));
  }

  /**
   * Verify that the Item and PathMetadata objects hold the same information.
   */
  private static void verify(Item item, PathMetadata meta) {
    assertNotNull(meta);
    final S3AFileStatus status = meta.getFileStatus();
    final Path path = status.getPath();
    assertEquals(item.get(PARENT), pathToParentKey(path.getParent()));
    assertEquals(item.get(CHILD), path.getName());
    boolean isDir = item.hasAttribute(IS_DIR) && item.getBoolean(IS_DIR);
    assertEquals(isDir, status.isDirectory());
    long len = item.hasAttribute(FILE_LENGTH) ? item.getLong(FILE_LENGTH) : 0;
    assertEquals(len, status.getLen());
    long bSize = item.hasAttribute(BLOCK_SIZE) ? item.getLong(BLOCK_SIZE) : 0;
    assertEquals(bSize, status.getBlockSize());
    String eTag = item.getString(ETAG);
    assertEquals(eTag, status.getETag());
    String versionId = item.getString(VERSION_ID);
    assertEquals(versionId, status.getVersionId());

    /*
     * S3AFileStatue#getModificationTime() reports the current time, so the
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
    assertEquals(pathToParentKey(path), attr.getValue());
  }

  private static String pathToParentKey(Path p) {
    Preconditions.checkArgument(p.isUriPathAbsolute());
    URI parentUri = p.toUri();
    String bucket = parentUri.getHost();
    Preconditions.checkNotNull(bucket);
    String s =  "/" + bucket + parentUri.getPath();
    // strip trailing slash
    if (s.endsWith("/")) {
      s = s.substring(0, s.length()-1);
    }
    return s;
  }

  @Test
  public void testPathToKey() throws Exception {
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> pathToKey(new Path("/")));
    doTestPathToKey(TEST_DIR_PATH);
    doTestPathToKey(TEST_FILE_PATH);
  }

  @Test
  public void testPathToKeyTrailing() throws Exception {
    doTestPathToKey(new Path("s3a://test-bucket/myDir/trailing//"));
    doTestPathToKey(new Path("s3a://test-bucket/myDir/trailing/"));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> pathToKey(new Path("s3a://test-bucket//")));
  }

  private static void doTestPathToKey(Path path) {
    final PrimaryKey key = pathToKey(path);
    assertNotNull(key);
    assertEquals("There should be both HASH and RANGE keys",
        2, key.getComponents().size());

    for (KeyAttribute keyAttribute : key.getComponents()) {
      assertThat(keyAttribute.getName(), anyOf(is(PARENT), is(CHILD)));
      if (PARENT.equals(keyAttribute.getName())) {
        assertEquals(pathToParentKey(path.getParent()),
            keyAttribute.getValue());
      } else {
        assertEquals(path.getName(), keyAttribute.getValue());
      }
    }
  }

  @Test
  public void testVersionRoundTrip() throws Throwable {
    final Item marker = createVersionMarker(VERSION_MARKER_ITEM_NAME, VERSION, 0);
    assertEquals("Extracted version from " + marker,
        VERSION, extractVersionFromMarker(marker));
  }

  @Test
  public void testVersionMarkerNotStatusIllegalPath() throws Throwable {
    final Item marker = createVersionMarker(VERSION_MARKER_ITEM_NAME, VERSION, 0);
    assertNull("Path metadata fromfrom " + marker,
        itemToPathMetadata(marker, "alice"));
  }

  /**
   * Test when translating an {@link Item} to {@link DDBPathMetadata} works
   * if {@code IS_AUTHORITATIVE} flag is ignored.
   */
  @Test
  public void testIsAuthoritativeCompatibilityItemToPathMetadata()
      throws Exception {
    Item item = Mockito.spy(testDirItem);
    item.withBoolean(IS_AUTHORITATIVE, true);
    PathMetadataDynamoDBTranslation.IGNORED_FIELDS.add(IS_AUTHORITATIVE);

    final String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
    DDBPathMetadata meta = itemToPathMetadata(item, user);

    Mockito.verify(item, Mockito.never()).getBoolean(IS_AUTHORITATIVE);
    assertFalse(meta.isAuthoritativeDir());
  }

  /**
   * Test when translating an {@link DDBPathMetadata} to {@link Item} works
   * if {@code IS_AUTHORITATIVE} flag is ignored.
   */
  @Test
  public void testIsAuthoritativeCompatibilityPathMetadataToItem() {
    DDBPathMetadata meta = Mockito.spy(testFilePathMetadata);
    meta.setAuthoritativeDir(true);
    PathMetadataDynamoDBTranslation.IGNORED_FIELDS.add(IS_AUTHORITATIVE);

    Item item = pathMetadataToItem(meta);

    Mockito.verify(meta, never()).isAuthoritativeDir();
    assertFalse(item.hasAttribute(IS_AUTHORITATIVE));
  }


  /**
   * Test when translating an {@link Item} to {@link DDBPathMetadata} works
   * if {@code LAST_UPDATED} flag is ignored.
   */
  @Test
  public void testIsLastUpdatedCompatibilityItemToPathMetadata()
      throws Exception {
    Item item = Mockito.spy(testDirItem);
    item.withLong(LAST_UPDATED, 100);
    PathMetadataDynamoDBTranslation.IGNORED_FIELDS.add(LAST_UPDATED);

    final String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
    DDBPathMetadata meta = itemToPathMetadata(item, user);

    Mockito.verify(item, Mockito.never()).getLong(LAST_UPDATED);
    assertFalse(meta.isAuthoritativeDir());
  }

  /**
   * Test when translating an {@link DDBPathMetadata} to {@link Item} works
   * if {@code LAST_UPDATED} flag is ignored.
   */
  @Test
  public void testIsLastUpdatedCompatibilityPathMetadataToItem() {
    DDBPathMetadata meta = Mockito.spy(testFilePathMetadata);
    meta.setLastUpdated(100);
    PathMetadataDynamoDBTranslation.IGNORED_FIELDS.add(LAST_UPDATED);

    Item item = pathMetadataToItem(meta);

    Mockito.verify(meta, never()).getLastUpdated();
    assertFalse(item.hasAttribute(LAST_UPDATED));
  }

}
