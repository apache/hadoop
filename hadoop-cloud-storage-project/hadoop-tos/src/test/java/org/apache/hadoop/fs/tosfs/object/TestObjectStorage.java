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

package org.apache.hadoop.fs.tosfs.object;

import com.volcengine.tos.TosServerException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.exceptions.InvalidObjectKeyException;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestObjectStorage {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestObjectStorage");
  private ObjectStorage storage;

  public static Stream<Arguments> provideArguments() {
    assumeTrue(TestEnv.checkTestEnabled());

    List<Arguments> values = new ArrayList<>();
    for (ObjectStorage store : TestUtility.createTestObjectStorage(FILE_STORE_ROOT)) {
      values.add(Arguments.of(store));
    }
    return values.stream();
  }

  private void setEnv(ObjectStorage objectStore) {
    this.storage = objectStore;
  }

  @AfterEach
  public void tearDown() {
    CommonUtils.runQuietly(() -> storage.deleteAll(""));
    for (MultipartUpload upload : storage.listUploads("")) {
      storage.abortMultipartUpload(upload.key(), upload.uploadId());
    }
  }

  @AfterAll
  public static void afterClass() throws Exception {
    CommonUtils.runQuietly(() -> TempFiles.deleteDir(FILE_STORE_ROOT));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testHeadNonExistObject(ObjectStorage store) {
    setEnv(store);
    assertNull(storage.head("a/b/c.txt"));

    byte[] data = TestUtility.rand(256);
    storage.put("a/b/c.txt", data);
    assertNotNull(storage.head("a/b/c.txt"));

    assertNull(storage.head("a/b/c/non-exits"));
    if (storage.bucket().isDirectory()) {
      assertThrows(InvalidObjectKeyException.class, () -> storage.head("a/b/c.txt/non-exits"));
    } else {
      assertNull(storage.head("a/b/c.txt/non-exits"));
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testHeadExistObject(ObjectStorage store) {
    setEnv(store);
    byte[] data = TestUtility.rand(256);
    String key = "testHeadExistObject.txt";
    storage.put(key, data);

    ObjectInfo obj = storage.head(key);
    assertEquals(key, obj.key());
    assertFalse(obj.isDir());
    if (storage.bucket().isDirectory()) {
      assertThrows(InvalidObjectKeyException.class, () -> storage.head(key + "/"));
    } else {
      assertNull(storage.head(key + "/"));
    }

    String dirKey = "testHeadExistObject/";
    storage.put(dirKey, new byte[0]);
    obj = storage.head(dirKey);
    assertEquals(dirKey, obj.key());
    assertTrue(obj.isDir());

    if (storage.bucket().isDirectory()) {
      obj = storage.head("testHeadExistObject");
      assertEquals("testHeadExistObject", obj.key());
      assertTrue(obj.isDir());
    } else {
      assertNull(storage.head("testHeadExistObject"));
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testGetAndDeleteNonExistFile(ObjectStorage store) {
    setEnv(store);
    // ensure file is not exist
    assertNull(storage.head("a/b/c.txt"));

    assertThrows(RuntimeException.class, () -> storage.get("a/b/c.txt", 0, 0));
    assertThrows(RuntimeException.class, () -> storage.get("a/b/c.txt", 0, 1));

    // Allow to delete a non-exist object.
    storage.delete("a/b/c.txt");
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutAndDeleteFileWithEmptyKey(ObjectStorage store) {
    setEnv(store);
    assertThrows(RuntimeException.class, () -> storage.put("", new byte[0]));
    assertThrows(RuntimeException.class, () -> storage.put(null, new byte[0]));
    assertThrows(RuntimeException.class, () -> storage.delete(null));
    assertThrows(RuntimeException.class, () -> storage.head(""));
    assertThrows(RuntimeException.class, () -> storage.head(null));
    assertThrows(RuntimeException.class, () -> getStream(""));
    assertThrows(RuntimeException.class, () -> getStream(null));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutObjectButContentLengthDisMatch(ObjectStorage store) throws IOException {
    setEnv(store);
    byte[] data = TestUtility.rand(256);
    String key = "a/truncated.txt";

    // The final object data will be truncated if content length is smaller.
    byte[] checksum = storage.put(key, () -> new ByteArrayInputStream(data), 200);
    assertArrayEquals(Arrays.copyOfRange(data, 0, 200), IOUtils.toByteArray(getStream(key)));
    ObjectInfo info = storage.head(key);
    assertEquals(key, info.key());
    assertEquals(200, info.size());
    assertArrayEquals(checksum, info.checksum());

    // Will create object failed is the content length is bigger.
    assertThrows(RuntimeException.class,
        () -> storage.put(key, () -> new ByteArrayInputStream(data), 300));
  }

  private InputStream getStream(String key) {
    return storage.get(key).stream();
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutAndGetFile(ObjectStorage store) throws IOException {
    setEnv(store);
    byte[] data = TestUtility.rand(256);
    String key = "a/test.txt";
    byte[] checksum = storage.put(key, data);
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));

    if (storage.bucket().isDirectory()) {
      // Directory bucket will create missed parent dir.
      assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream("a")));
      assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream("a/")));
    } else {
      assertNull(storage.head("a"));
      assertNull(storage.head("a/"));
    }

    ObjectInfo info = storage.head(key);
    assertEquals(key, info.key());
    assertEquals(data.length, info.size());
    assertArrayEquals(checksum, info.checksum());

    ObjectContent content = storage.get(key);
    assertArrayEquals(info.checksum(), content.checksum());
    assertArrayEquals(data, IOUtils.toByteArray(content.stream()));

    assertArrayEquals(data, IOUtils.toByteArray(getStream(key, 0, -1)));
    assertThrows(RuntimeException.class, () -> storage.get(key, -1, -1), "offset is negative");
    assertThrows(RuntimeException.class, () -> storage.get(key + "/", 0, -1),
        "path not found or resource type is invalid");

    assertArrayEquals(data, IOUtils.toByteArray(getStream(key, 0, 256)));
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key, 0, 512)));

    byte[] secondHalfData = Arrays.copyOfRange(data, 128, 256);
    assertArrayEquals(secondHalfData, IOUtils.toByteArray(getStream(key, 128, -1)));
    assertArrayEquals(secondHalfData, IOUtils.toByteArray(getStream(key, 128, 256)));
    assertArrayEquals(secondHalfData, IOUtils.toByteArray(getStream(key, 128, 257)));
    assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream(key, 128, 0)));

    ObjectContent partContent = storage.get(key, 8, 32);
    assertArrayEquals(info.checksum(), partContent.checksum());
    assertArrayEquals(Arrays.copyOfRange(data, 8, 40),
        IOUtils.toByteArray(partContent.stream()));
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));

    assertThrows(RuntimeException.class, () -> storage.get(key, 257, 8),
        "offset is bigger than object length");
    assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream(key, 256, 8)));

    assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream(key, 0, 0)));
    assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream(key, 1, 0)));


    // assert the original data is not changed during random get request
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));

    storage.delete(key);
    assertNull(storage.head(key));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testAppendAndGetFile(ObjectStorage store) throws Exception {
    setEnv(store);
    String key = "a/testAppendAndGetFile.txt";

    // Append zero bytes.
    assertThrows(NotAppendableException.class, () -> storage.append(key, new byte[0]),
        "Append non-existed object with zero byte is not supported.");

    // Append 256 bytes.
    byte[] data = TestUtility.rand(256);
    byte[] checksum = storage.append(key, data);
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));

    // Append zero bytes.
    byte[] newChecksum = storage.append(key, new byte[0]);
    assertArrayEquals(checksum, newChecksum);
    checksum = newChecksum;

    // Append one byte.
    newChecksum = storage.append(key, new byte[1]);
    assertFalse(Arrays.equals(checksum, newChecksum));
    assertArrayEquals(newChecksum, storage.head(key).checksum());
    checksum = newChecksum;

    // Append 1024 byte.
    data = TestUtility.rand(1024);
    newChecksum = storage.append(key, data);
    assertFalse(Arrays.equals(checksum, newChecksum));
    assertArrayEquals(newChecksum, storage.head(key).checksum());

    storage.delete(key);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testAppendLengthNotMatch(ObjectStorage store) {
    setEnv(store);
    byte[] data = TestUtility.rand(256);
    String key = "a/testAppendLengthNotMatch.txt";
    storage.append(key, () -> new ByteArrayInputStream(data), 128);
    assertEquals(128, storage.head(key).size());

    assertThrows(RuntimeException.class,
        () -> storage.append(key, () -> new ByteArrayInputStream(data), 1024),
        "Expect unexpected end of stream error.");
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testHeadAndListAndObjectStatusShouldGetSameObjectInfo(ObjectStorage store) {
    setEnv(store);
    String key = "testHeadAndListObjectCheckSum.txt";
    byte[] data = TestUtility.rand(256);
    byte[] checksum = storage.put(key, data);

    ObjectInfo obj = storage.head(key);
    assertEquals(obj, storage.objectStatus(key));
    if (!storage.bucket().isDirectory()) {
      List<ObjectInfo> objects = toList(storage.list(key, null, 1));
      assertEquals(1, objects.size());
      assertEquals(obj, objects.get(0));
      assertArrayEquals(checksum, objects.get(0).checksum());
    }


    key = "testHeadAndListObjectCheckSum/";
    checksum = storage.put(key, new byte[0]);
    obj = storage.head(key);
    assertEquals(obj, storage.objectStatus(key));
    if (!storage.bucket().isDirectory()) {
      List<ObjectInfo> objects = toList(storage.list(key, null, 1));
      assertEquals(1, objects.size());
      assertEquals(obj, objects.get(0));
      assertArrayEquals(checksum, objects.get(0).checksum());
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testObjectStatus(ObjectStorage store) {
    setEnv(store);
    // test get file status
    String key = "a/b/testObjectStatus.txt";
    byte[] data = TestUtility.rand(256);
    byte[] checksum = storage.put(key, data);

    ObjectInfo obj = storage.head(key);
    assertArrayEquals(checksum, obj.checksum());
    assertEquals(obj, storage.objectStatus(key));

    if (storage.bucket().isDirectory()) {
      assertThrows(InvalidObjectKeyException.class, () -> storage.head(key + "/"));
      assertThrows(InvalidObjectKeyException.class, () -> storage.objectStatus(key + "/"));
    } else {
      assertNull(storage.head(key + "/"));
      assertNull(storage.objectStatus(key + "/"));
    }

    // test get dir status
    String dirKey = "a/b/dir/";
    checksum = storage.put(dirKey, new byte[0]);
    obj = storage.head(dirKey);
    assertEquals(Constants.MAGIC_CHECKSUM, checksum);
    assertArrayEquals(Constants.MAGIC_CHECKSUM, checksum);
    assertArrayEquals(checksum, obj.checksum());
    assertTrue(obj.isDir());
    assertEquals(dirKey, obj.key());
    assertEquals(obj, storage.objectStatus(dirKey));

    if (storage.bucket().isDirectory()) {
      assertNotNull(storage.head("a/b/dir"));
      assertEquals("a/b/dir", storage.objectStatus("a/b/dir").key());
    } else {
      assertNull(storage.head("a/b/dir"));
      assertEquals(dirKey, storage.objectStatus("a/b/dir").key());
    }

    // test get dir status of prefix
    String prefix = "a/b/";
    obj = storage.objectStatus(prefix);
    assertEquals(prefix, obj.key());
    assertEquals(Constants.MAGIC_CHECKSUM, obj.checksum());
    assertTrue(obj.isDir());

    if (storage.bucket().isDirectory()) {
      assertEquals(obj, storage.head(prefix));
      assertEquals("a/b", storage.objectStatus("a/b").key());
    } else {
      assertNull(storage.head(prefix));
      assertEquals(prefix, storage.objectStatus("a/b").key());
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutAndGetDirectory(ObjectStorage store) throws IOException {
    setEnv(store);
    String key = "a/b/";
    byte[] data = new byte[0];
    storage.put(key, data);

    ObjectInfo info = storage.head(key);
    assertEquals(key, info.key());
    assertEquals(data.length, info.size());
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));
    assertArrayEquals(data, IOUtils.toByteArray(getStream(key, 0, 256)));

    // test create the same dir again
    storage.put(key, data);

    storage.delete(key);
    assertNull(storage.head(key));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testOverwriteFile(ObjectStorage store) throws IOException {
    setEnv(store);
    String key = "a/test.txt";
    byte[] data1 = TestUtility.rand(256);
    byte[] data2 = TestUtility.rand(128);

    storage.put(key, data1);
    assertArrayEquals(data1, IOUtils.toByteArray(getStream(key, 0, -1)));

    storage.put(key, data2);
    assertArrayEquals(data2, IOUtils.toByteArray(getStream(key, 0, -1)));

    storage.delete(key);
    assertNull(storage.head(key));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectsWithEmptyDelimiters(ObjectStorage store) {
    setEnv(store);
    // Directory bucket only supports list with delimiter = '/' currently.
    assumeFalse(storage.bucket().isDirectory());
    String key1 = "a/b/c/d";
    String key2 = "a/b";

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%s/file-%d.txt", key1, i), data);
      storage.put(String.format("%s/file-%d.txt", key2, i), data);
    }

    // list 100 objects under 'a/', there are total 20 objects.
    ListObjectsResponse response = list("a/", "", 100, "");
    assertEquals(20, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/c/d/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(19).key());

    // list 20 objects and there only have 20 objects under 'a/'
    response = list("a/", "", 20, "");
    assertEquals(20, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/c/d/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(19).key());

    // list the top 10 objects among 20 objects
    response = list("a/", "", 10, "");
    assertEquals(10, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/c/d/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/c/d/file-9.txt", response.objects().get(9).key());

    // list the next 5 objects behind a/b/c/d/file-9.txt among 20 objects
    response = list("a/", "a/b/c/d/file-9.txt", 5, "");
    assertEquals(5, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-4.txt", response.objects().get(4).key());

    // list the next 10 objects behind a/b/c/d/file-9.txt among 20 objects
    response = list("a/", "a/b/c/d/file-9.txt", 10, "");
    assertEquals(10, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(9).key());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListEmptyDirWithSlashDelimiter(ObjectStorage store) {
    setEnv(store);
    String key = "a/b/";
    storage.put(key, new byte[0]);

    ListObjectsResponse response = list(key, null, 10, "/");
    assertEquals(1, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/", response.objects().get(0).key());

    response = list(key, key, 10, "/");
    assertEquals(0, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDeleteMultipleKeys(ObjectStorage store) {
    setEnv(store);
    String prefix = "a/b";
    byte[] data = TestUtility.rand(256);

    List<String> keys = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      String existingKey = String.format("%s/existing-file-%d.txt", prefix, i);
      storage.put(existingKey, data);
      keys.add(existingKey);

      String unExistingKey = String.format("%s/unExisting-file-%d.txt", prefix, i);
      keys.add(unExistingKey);
    }

    List<String> failedKeys = storage.batchDelete(keys);

    for (String key : failedKeys) {
      assertNotNull(storage.head(key));
    }

    for (String key : keys) {
      if (!failedKeys.contains(key)) {
        assertNull(storage.head(key));
      }
    }

    assertThrows(IllegalArgumentException.class, () -> storage.batchDelete(
            IntStream.range(0, 1001).mapToObj(String::valueOf).collect(Collectors.toList())),
        "The deleted keys size should be <= 1000");
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectsWithEmptyMarkers(ObjectStorage store) {
    setEnv(store);
    String key1 = "a/b/c/d";
    String key2 = "a/b";
    String key3 = "a1/b1";

    // create the folder to compatible with directory bucket.
    storage.put("a/", new byte[0]);
    storage.put("a/b/", new byte[0]);
    storage.put("a/b/c/", new byte[0]);
    storage.put("a/b/c/d/", new byte[0]);
    storage.put("a1/", new byte[0]);
    storage.put("a1/b1/", new byte[0]);

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%s/file-%d.txt", key1, i), data);
      storage.put(String.format("%s/file-%d.txt", key2, i), data);
      storage.put(String.format("%s/file-%d.txt", key3, i), data);
    }

    // group objects by '/' under 'a/'
    ListObjectsResponse response = list("a/", null, 100, "/");
    assertEquals(1, response.objects().size());
    assertEquals("a/", response.objects().get(0).key());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));

    response = list("a", null, 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(2, response.commonPrefixes().size());
    assertEquals("a/", response.commonPrefixes().get(0));
    assertEquals("a1/", response.commonPrefixes().get(1));

    // group objects by '/' under 'a/b/' and group objects by 'b/' under 'a', they are same
    response = list("a/b/", null, 100, "/");
    assertEquals(11, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/", response.objects().get(0).key());
    assertEquals("a/b/file-0.txt", response.objects().get(1).key());
    assertEquals("a/b/file-9.txt", response.objects().get(10).key());

    response = list("a/b", null, 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));

    if (!storage.bucket().isDirectory()) {
      // Directory bucket only supports list with delimiter = '/' currently.
      response = list("a", null, 100, "b/");
      assertEquals(13, response.objects().size());
      assertEquals(1, response.commonPrefixes().size());
      assertEquals("a/b/", response.commonPrefixes().get(0));
      assertEquals("a/", response.objects().get(0).key());
      assertEquals("a1/", response.objects().get(1).key());
      assertEquals("a1/b1/", response.objects().get(2).key());
      assertEquals("a1/b1/file-0.txt", response.objects().get(3).key());
      assertEquals("a1/b1/file-9.txt", response.objects().get(12).key());

      response = list("a/", null, 100, "b/");
      assertEquals(1, response.objects().size());
      assertEquals(1, response.commonPrefixes().size());
      assertEquals("a/b/", response.commonPrefixes().get(0));
      assertEquals("a/", response.objects().get(0).key());
    }

    // group objects by different delimiter under 'a/b/c/d/' or 'a/b/c/d'
    response = list("a/b/c/d/", null, 100, "/");
    assertEquals(11, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/c/d/", response.objects().get(0).key());

    response = list("a/b/c/d/", null, 5, "/");
    assertEquals(5, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a/b/c/d/", response.objects().get(0).key());

    response = list("a/b/c/d", null, 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/c/d/", response.commonPrefixes().get(0));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectWithLimitObjectAndCommonPrefixes(ObjectStorage store) {
    setEnv(store);
    String key1 = "a/b/c/d";
    String key2 = "a/b";
    String key3 = "a1/b1";

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%s/file-%d.txt", key1, i), data);
      storage.put(String.format("%s/file-%d.txt", key2, i), data);
      storage.put(String.format("%s/file-%d.txt", key3, i), data);
    }

    List<String> dirKeys = Lists.newArrayList("a/b/d/", "a/b/e/", "a/b/f/", "a/b/g/");
    for (String key : dirKeys) {
      storage.put(key, new byte[0]);
    }

    // group objects by '/' under 'a/b/', and limit top 5 objects among 10 objects and 1 common
    // prefix.
    ListObjectsResponse response = list("a/b/", "a/b/", 5, "/");
    assertEquals(1, response.objects().size());
    assertEquals(4, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
    assertEquals("a/b/e/", response.commonPrefixes().get(2));
    assertEquals("a/b/f/", response.commonPrefixes().get(3));
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());

    response = list("a/b/", "a/b/", 14, "/");
    assertEquals(10, response.objects().size());
    assertEquals(4, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
    assertEquals("a/b/e/", response.commonPrefixes().get(2));
    assertEquals("a/b/f/", response.commonPrefixes().get(3));
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(9).key());

    response = list("a/b/", "a/b/", 15, "/");
    assertEquals(10, response.objects().size());
    assertEquals(5, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
    assertEquals("a/b/e/", response.commonPrefixes().get(2));
    assertEquals("a/b/f/", response.commonPrefixes().get(3));
    assertEquals("a/b/g/", response.commonPrefixes().get(4));
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(9).key());

    // a/b/h-file-0.txt is behind from a/b/g/
    storage.put("a/b/h-file-0.txt", data);
    response = list("a/b/", "a/b/", 15, "/");
    assertEquals(10, response.objects().size());
    assertEquals(5, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
    assertEquals("a/b/e/", response.commonPrefixes().get(2));
    assertEquals("a/b/f/", response.commonPrefixes().get(3));
    assertEquals("a/b/g/", response.commonPrefixes().get(4));
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/file-9.txt", response.objects().get(9).key());

    response = list("a/b/", "a/b/", 20, "/");
    assertEquals(11, response.objects().size());
    assertEquals(5, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
    assertEquals("a/b/e/", response.commonPrefixes().get(2));
    assertEquals("a/b/f/", response.commonPrefixes().get(3));
    assertEquals("a/b/g/", response.commonPrefixes().get(4));
    assertEquals("a/b/file-0.txt", response.objects().get(0).key());
    assertEquals("a/b/h-file-0.txt", response.objects().get(10).key());

    response = list("a/b/", "a/b/", 1, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));

    response = list("a/b/", "a/b/", 2, "/");
    assertEquals(0, response.objects().size());
    assertEquals(2, response.commonPrefixes().size());
    assertEquals("a/b/c/", response.commonPrefixes().get(0));
    assertEquals("a/b/d/", response.commonPrefixes().get(1));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListedIteratorIsIdempotent(ObjectStorage store) {
    setEnv(store);
    String key1 = "a/b/c/d";

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%s/file-%d.txt", key1, i), data);
    }

    Iterable<ObjectInfo> res;
    if (storage.bucket().isDirectory()) {
      res = ((DirectoryStorage) storage).listDir("a/b/c/d/", true);
    } else {
      res = storage.list("a/b/c/d/", "a/b/c/d/", 10);
    }
    Iterator<ObjectInfo> batch1 = res.iterator();
    Iterator<ObjectInfo> batch2 = res.iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(batch1.hasNext());
      ObjectInfo obj = batch1.next();
      assertEquals(String.format("a/b/c/d/file-%d.txt", i), obj.key());
    }
    assertFalse(batch1.hasNext());

    for (int i = 0; i < 10; i++) {
      assertTrue(batch2.hasNext());
      ObjectInfo obj = batch2.next();
      assertEquals(String.format("a/b/c/d/file-%d.txt", i), obj.key());
    }
    assertFalse(batch2.hasNext());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectsWithSmallBatch(ObjectStorage store) {
    setEnv(store);
    assumeFalse(storage.bucket().isDirectory());
    String key1 = "a/b/c/d/";

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%sfile-%d.txt", key1, i), data);
    }

    // change list object count
    Configuration newConf = new Configuration(storage.conf());
    newConf.setInt(TosKeys.FS_TOS_LIST_OBJECTS_COUNT, 5);
    storage.initialize(newConf, storage.bucket().name());

    List<Integer> maxKeys = Arrays.asList(5, 10, 9, 20, -1);
    for (int maxKey : maxKeys) {
      Iterator<ObjectInfo> objs = storage.list(key1, key1, maxKey).iterator();
      int end = Math.min(maxKey == -1 ? 10 : maxKey, 10);
      for (int i = 0; i < end; i++) {
        assertTrue(objs.hasNext());
        ObjectInfo obj = objs.next();
        assertEquals(String.format("a/b/c/d/file-%d.txt", i), obj.key());
      }
      assertFalse(objs.hasNext());
    }

    // reset list object count
    newConf = new Configuration(storage.conf());
    newConf.setInt(TosKeys.FS_TOS_LIST_OBJECTS_COUNT, 1000);
    storage.initialize(newConf, storage.bucket().name());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectsWithSpecificDelimiters(ObjectStorage store) {
    setEnv(store);
    assumeFalse(storage.bucket().isDirectory());
    String key1 = "a/b/c/d";
    String key2 = "a/b";
    String key3 = "a1/b1";

    byte[] data = TestUtility.rand(256);
    for (int i = 0; i < 10; i++) {
      storage.put(String.format("%s/file-%d.txt", key1, i), data);
      storage.put(String.format("%s/file-%d.txt", key2, i), data);
      storage.put(String.format("%s/file-%d.txt", key3, i), data);
    }

    ListObjectsResponse response = list("a", "", 11, "b/");
    assertEquals(10, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));
    assertEquals("a1/b1/file-0.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-9.txt", response.objects().get(9).key());

    response = list("a", "", 5, "b/");
    assertEquals(4, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));
    assertEquals("a1/b1/file-0.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-3.txt", response.objects().get(3).key());

    response = list("a", "a1/b1/file-3.txt", 5, "b/");
    assertEquals(5, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a1/b1/file-4.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-8.txt", response.objects().get(4).key());

    response = list("a", "a1/b1/file-3.txt", 6, "b/");
    assertEquals(6, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());
    assertEquals("a1/b1/file-4.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-9.txt", response.objects().get(5).key());

    response = list("a", "a/b/file-3.txt", 5, "b/");
    assertEquals(4, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));
    assertEquals("a1/b1/file-0.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-3.txt", response.objects().get(3).key());

    response = list("a", "a/b/file-3.txt", 10, "b/");
    assertEquals(9, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));
    assertEquals("a1/b1/file-0.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-8.txt", response.objects().get(8).key());

    response = list("a", "a/b/file-3.txt", 11, "b/");
    assertEquals(10, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/", response.commonPrefixes().get(0));
    assertEquals("a1/b1/file-0.txt", response.objects().get(0).key());
    assertEquals("a1/b1/file-9.txt", response.objects().get(9).key());

    response = list("a", "a/b/", 1, "b/");
    assertEquals(1, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());

    response = list("a/b/c/d", "", 100, "/file");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/c/d/file", response.commonPrefixes().get(0));

    response = list("a/b/c/d/", "", 100, "file");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a/b/c/d/file", response.commonPrefixes().get(0));


    // group objects by different delimiter under 'a1' or 'a1/'
    response = list("a1", "", 100, "");
    assertEquals(10, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());

    response = list("a1", "", 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a1/", response.commonPrefixes().get(0));

    response = list("a1/", "", 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a1/b1/", response.commonPrefixes().get(0));

    response = list("a1/", "", 1, "/");
    assertEquals(0, response.objects().size());
    assertEquals(1, response.commonPrefixes().size());
    assertEquals("a1/b1/", response.commonPrefixes().get(0));

    // group objects by non-exist delimiter under 'a1' or 'a1/'
    response = list("a1", "", 100, "non-exist");
    assertEquals(10, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());

    response = list("a1/", "", 100, "non-exist");
    assertEquals(10, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());

    // test the sequent of common prefixes
    response = list("a", "", 100, "b");
    assertEquals(0, response.objects().size());
    assertEquals(2, response.commonPrefixes().size());
    assertEquals("a/b", response.commonPrefixes().get(0));
    assertEquals("a1/b", response.commonPrefixes().get(1));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testOverwriteDirectoryWithAFile(ObjectStorage store) throws IOException {
    setEnv(store);
    String dirKey = "a/b/";
    String key = "a/b";
    storage.delete("a/");

    byte[] data1 = new byte[0];
    byte[] data2 = TestUtility.rand(128);

    storage.put(dirKey, data1);
    assertArrayEquals(data1, IOUtils.toByteArray(getStream(dirKey, 0, 256)));

    if (!storage.bucket().isDirectory()) {
      // Directory bucket doesn't allow overwrote if the resource type is changed.
      storage.put(key, data2);
      assertArrayEquals(data2, IOUtils.toByteArray(getStream(key, 0, 256)));
    }

    storage.delete(key);
    storage.delete(dirKey);
    assertNull(storage.head(key));
    assertNull(storage.head(dirKey));
  }

  private InputStream getStream(String key, long off, long limit) {
    return storage.get(key, off, limit).stream();
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testDeleteNonEmptyDir(ObjectStorage store) throws IOException {
    setEnv(store);
    storage.put("a/", new byte[0]);
    storage.put("a/b/", new byte[0]);
    assertArrayEquals(new byte[0], IOUtils.toByteArray(getStream("a/b/", 0, 256)));

    ListObjectsResponse response = list("a/b/", "a/b/", 100, "/");
    assertEquals(0, response.objects().size());
    assertEquals(0, response.commonPrefixes().size());

    if (!storage.bucket().isDirectory()) {
      // Directory bucket only supports list with delimiter = '/'.
      response = list("a/b/", "a/b/", 100, null);
      assertEquals(0, response.objects().size());
      assertEquals(0, response.commonPrefixes().size());
    }

    storage.delete("a/b/");
    assertNull(storage.head("a/b/"));
    assertNull(storage.head("a/b"));
    assertNotNull(storage.head("a/"));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRecursiveDelete(ObjectStorage store) {
    setEnv(store);
    storage.put("a/", new byte[0]);
    storage.put("a/b/", new byte[0]);
    storage.put("a/b/c1/", new byte[0]);
    storage.put("a/b/c2/", new byte[0]);
    storage.put("a/b/c3/", new byte[0]);
    assertNotNull(storage.head("a/"));
    assertNotNull(storage.head("a/b/"));
    assertNotNull(storage.head("a/b/c1/"));
    assertNotNull(storage.head("a/b/c2/"));
    assertNotNull(storage.head("a/b/c3/"));

    storage.delete("a/b/c3/");
    assertNull(storage.head("a/b/c3/"));

    storage.deleteAll("");
    assertNull(storage.head("a/b/c1/"));
    assertNull(storage.head("a/b/c2/"));
    assertNull(storage.head("a/b/"));
    assertNull(storage.head("a/"));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListObjectKeys(ObjectStorage store) {
    setEnv(store);
    assumeFalse(storage.bucket().isDirectory());
    byte[] dirBytes = new byte[0];
    byte[] fileBytes = TestUtility.rand(128);
    storage.put("a/b1/", dirBytes);
    storage.put("a/b2/c0/", dirBytes);
    storage.put("a/b2/c1/d1.txt", fileBytes);
    storage.put("a/b2/c1/e1.txt", fileBytes);
    storage.put("a/b2/c2.txt", fileBytes);

    // list single dir
    List<ObjectInfo> ret = toList(storage.list("a/b1", "", 10));
    assertEquals(1, ret.size());
    assertEquals("a/b1/", ret.get(0).key());
    assertEquals(0, ret.get(0).size());

    ret = toList(storage.list("a/b1/", "", 10));
    assertEquals(1, ret.size());
    assertEquals("a/b1/", ret.get(0).key());
    assertEquals(0, ret.get(0).size());

    // list single file
    ret = toList(storage.list("a/b2/c1/d1.txt", "", 10));
    assertEquals(1, ret.size());
    assertEquals("a/b2/c1/d1.txt", ret.get(0).key());
    assertEquals(fileBytes.length, ret.get(0).size());

    // list multiple files & dirs
    ret = toList(storage.list("a/b2", "", 10));
    assertEquals(4, ret.size());
    assertEquals("a/b2/c0/", ret.get(0).key());
    assertEquals("a/b2/c1/d1.txt", ret.get(1).key());
    assertEquals("a/b2/c1/e1.txt", ret.get(2).key());
    assertEquals("a/b2/c2.txt", ret.get(3).key());
    assertEquals(dirBytes.length, ret.get(0).size());

    // list single file with marker
    ret = toList(storage.list("a/b2", "a/b2/c1/e1.txt", 10));
    assertEquals(1, ret.size());
    assertEquals("a/b2/c2.txt", ret.get(0).key());
    assertEquals(fileBytes.length, ret.get(0).size());

    // list multiple files with marker
    ret = toList(storage.list("a/b2", "a/b2/c1/", 10));
    assertEquals(3, ret.size());
    assertEquals("a/b2/c1/d1.txt", ret.get(0).key());
    assertEquals("a/b2/c1/e1.txt", ret.get(1).key());
    assertEquals("a/b2/c2.txt", ret.get(2).key());
    assertEquals(fileBytes.length, ret.get(0).size());

    // list multiple files & dirs with part path as prefix
    ret = toList(storage.list("a/b2/c", "", 10));
    assertEquals(4, ret.size());
    assertEquals("a/b2/c0/", ret.get(0).key());
    assertEquals("a/b2/c1/d1.txt", ret.get(1).key());
    assertEquals("a/b2/c1/e1.txt", ret.get(2).key());
    assertEquals("a/b2/c2.txt", ret.get(3).key());
    assertEquals(dirBytes.length, ret.get(0).size());

    ret = toList(storage.list("a/b2/c", "", 2));
    assertEquals(2, ret.size());
    assertEquals("a/b2/c0/", ret.get(0).key());

    ret = toList(storage.list("a/b2/c1/d1.", "", 10));
    assertEquals(1, ret.size());
    assertEquals("a/b2/c1/d1.txt", ret.get(0).key());
    assertEquals(fileBytes.length, ret.get(0).size());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListAllObjectKeys(ObjectStorage store) {
    setEnv(store);
    assumeFalse(storage.bucket().isDirectory());
    byte[] dirBytes = new byte[0];
    byte[] fileBytes = TestUtility.rand(128);
    storage.put("a/b1/", dirBytes);
    storage.put("a/b2/c0/", dirBytes);
    storage.put("a/b2/c1/d1.txt", fileBytes);
    storage.put("a/b2/c1/e1.txt", fileBytes);
    storage.put("a/b2/c2.txt", dirBytes);

    // list single dir
    List<ObjectInfo> ret = Lists.newArrayList(storage.listAll("a/b1", ""));
    assertEquals(1, ret.size());
    assertEquals("a/b1/", ret.get(0).key());
    assertEquals(0, ret.get(0).size());

    // list single file
    ret = Lists.newArrayList(storage.listAll("a/b2/c1/d1.txt", ""));
    assertEquals(1, ret.size());
    assertEquals("a/b2/c1/d1.txt", ret.get(0).key());
    assertEquals(fileBytes.length, ret.get(0).size());

    // list multiple files & dirs
    ret = Lists.newArrayList(storage.listAll("a/b2", ""));
    assertEquals(4, ret.size());
    assertEquals("a/b2/c0/", ret.get(0).key());
    assertEquals("a/b2/c1/d1.txt", ret.get(1).key());
    assertEquals("a/b2/c1/e1.txt", ret.get(2).key());
    assertEquals("a/b2/c2.txt", ret.get(3).key());
    assertEquals(dirBytes.length, ret.get(0).size());

    // list multiple files & dirs with part path as prefix
    ret = Lists.newArrayList(storage.listAll("a/b2/c", ""));
    assertEquals(4, ret.size());
    assertEquals("a/b2/c0/", ret.get(0).key());
    assertEquals("a/b2/c1/d1.txt", ret.get(1).key());
    assertEquals("a/b2/c1/e1.txt", ret.get(2).key());
    assertEquals("a/b2/c2.txt", ret.get(3).key());
    assertEquals(dirBytes.length, ret.get(0).size());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListEmptyKeys(ObjectStorage store) {
    setEnv(store);
    if (storage.bucket().isDirectory()) {
      assertEquals(0,
          Lists.newArrayList(((DirectoryStorage) storage).listDir("not-exist", true)).size());
    } else {
      assertEquals(0, Lists.newArrayList(storage.list("not-exist", "", 2)).size());
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testMultiUploadEmptyFile(ObjectStorage store) {
    setEnv(store);
    String key = "a/b/empty.txt";
    MultipartUpload upload = storage.createMultipartUpload(key);
    assertThrows(Exception.class,
        () -> storage.completeUpload(key, upload.uploadId(), Lists.newArrayList()));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testMultiUploadZeroByte(ObjectStorage store) throws IOException {
    setEnv(store);
    String key = "a/b/zero.txt";
    MultipartUpload upload = storage.createMultipartUpload(key);
    Part part =
        storage.uploadPart(key, upload.uploadId(), 1, () -> new ByteArrayInputStream(new byte[0]),
            0);
    storage.completeUpload(key, upload.uploadId(), Lists.newArrayList(part));
    assertArrayEquals(ObjectTestUtils.EMPTY_BYTES, IOUtils.toByteArray(getStream(key)));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testMultiUploadFile(ObjectStorage store) throws IOException {
    setEnv(store);
    String key1 = "a/b/c/e.txt";
    String uploadId1 = storage.createMultipartUpload(key1).uploadId();
    assertNotEquals(uploadId1, "");

    byte[] dataset = multipleUpload(key1, uploadId1, 2, true);
    assertArrayEquals(dataset, IOUtils.toByteArray(getStream(key1)));

    String key2 = "a/b/e/e.txt";
    String uploadId2 = storage.createMultipartUpload(key2).uploadId();
    assertNotEquals(uploadId2, "");

    dataset = multipleUpload(key2, uploadId2, 3, true);
    assertArrayEquals(dataset, IOUtils.toByteArray(getStream(key2)));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testPutAndCompleteMPUWithSameContent(ObjectStorage store) throws IOException {
    setEnv(store);
    String mpu = "a/b/mpu.txt";
    String put = "a/b/put.txt";
    byte[] dataset = TestUtility.rand(11 << 20);
    byte[] checksum = multipleUpload(mpu, dataset);

    storage.put(put, dataset);

    ObjectInfo mputObj = storage.head(mpu);
    ObjectInfo putObj = storage.head(put);
    assertArrayEquals(checksum, mputObj.checksum());
    assertArrayEquals(checksum, putObj.checksum());

    if (!storage.bucket().isDirectory()) {
      List<ObjectInfo> objectInfo = toList(storage.list(mpu, null, 10));
      assertEquals(mputObj, objectInfo.get(0));
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testListUploads(ObjectStorage store) {
    setEnv(store);
    String key1 = "a/b/c/e.txt";
    String uploadId1 = storage.createMultipartUpload(key1).uploadId();
    assertNotEquals(uploadId1, "");
    multipleUpload(key1, uploadId1, 2, false);

    String key2 = "a/b/e/e.txt";
    String uploadId2 = storage.createMultipartUpload(key2).uploadId();
    assertNotEquals(uploadId2, "");
    multipleUpload(key2, uploadId2, 3, false);

    Iterable<MultipartUpload> iterable = storage.listUploads("");
    List<MultipartUpload> uploads = Lists.newArrayList(iterable.iterator());
    assertEquals(2, uploads.size());
    assertEquals(key1, uploads.get(0).key());
    assertEquals(uploadId1, uploads.get(0).uploadId());
    assertEquals(key2, uploads.get(1).key());
    assertEquals(uploadId2, uploads.get(1).uploadId());

    // check iterator is idempotent
    uploads = Lists.newArrayList(iterable.iterator());
    assertEquals(2, uploads.size());
    assertEquals(key1, uploads.get(0).key());
    assertEquals(uploadId1, uploads.get(0).uploadId());
    assertEquals(key2, uploads.get(1).key());
    assertEquals(uploadId2, uploads.get(1).uploadId());

    uploads = Lists.newArrayList(storage.listUploads("a/b/"));
    assertEquals(2, uploads.size());
    assertEquals(key1, uploads.get(0).key());
    assertEquals(uploadId1, uploads.get(0).uploadId());
    assertEquals(key2, uploads.get(1).key());
    assertEquals(uploadId2, uploads.get(1).uploadId());

    uploads = Lists.newArrayList(storage.listUploads("a/b/c/"));
    assertEquals(1, uploads.size());
    assertEquals(key1, uploads.get(0).key());
    assertEquals(uploadId1, uploads.get(0).uploadId());

    storage.abortMultipartUpload(key1, uploadId1);
    storage.abortMultipartUpload(key2, uploadId2);
    assertEquals(0, Lists.newArrayList((storage.listUploads("a/b/"))).size());
  }

  private byte[] multipleUpload(String key, String uploadId, int partCnt, boolean completeUpload) {
    int partSize = 5 * 1024 * 1024;
    byte[] dataset = new byte[partCnt * partSize];
    byte[] partData = TestUtility.rand(partSize);
    try {
      int offset = 0;
      List<Part> parts = new ArrayList<>();
      for (int i = 1; i <= partCnt; i++) {
        Part part = storage.uploadPart(key, uploadId, i, () -> new ByteArrayInputStream(partData),
            partData.length);
        parts.add(part);
        System.arraycopy(partData, 0, dataset, offset, partData.length);
        offset += partData.length;
      }
      if (completeUpload) {
        storage.completeUpload(key, uploadId, parts);
      }
    } catch (RuntimeException e) {
      storage.abortMultipartUpload(key, uploadId);
    }
    return dataset;
  }

  private byte[] multipleUpload(String key, byte[] dataset) throws IOException {
    int partSize = 5 * 1024 * 1024;
    int partCnt = (int) Math.ceil((double) dataset.length / partSize);

    String uploadId = storage.createMultipartUpload(key).uploadId();
    assertNotEquals(uploadId, "");

    try {
      List<Part> parts = new ArrayList<>();
      for (int i = 0; i < partCnt; i++) {
        int start = i * partSize;
        int end = Math.min(dataset.length, start + partSize);
        byte[] partData = Arrays.copyOfRange(dataset, start, end);

        Part part =
            storage.uploadPart(key, uploadId, i + 1, () -> new ByteArrayInputStream(partData),
                partData.length);

        assertEquals(DigestUtils.md5Hex(partData), part.eTag().replace("\"", ""));
        parts.add(part);
      }

      byte[] checksum = storage.completeUpload(key, uploadId, parts);
      assertArrayEquals(dataset, IOUtils.toByteArray(getStream(key)));

      return checksum;
    } catch (IOException | RuntimeException e) {
      storage.abortMultipartUpload(key, uploadId);
      throw e;
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testUploadPartCopy10MB(ObjectStorage store) {
    setEnv(store);
    String srcKey = "src10MB.txt";
    String dstKey = "dst10MB.txt";
    testUploadPartCopy(srcKey, dstKey, 10 << 20); // 10MB
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testUploadPartCopy100MB(ObjectStorage store) {
    setEnv(store);
    String srcKey = "src100MB.txt";
    String dstKey = "dst100MB.txt";
    testUploadPartCopy(srcKey, dstKey, 100 << 20); // 100MB
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testUploadPartCopy65MB(ObjectStorage store) {
    setEnv(store);
    String srcKey = "src65MB.txt";
    String dstKey = "dst65MB.txt";
    testUploadPartCopy(srcKey, dstKey, 65 << 20); // 65MB
  }

  private void testUploadPartCopy(String srcKey, String key, int fileSize) {
    MultipartUpload srcMultipartUpload = storage.createMultipartUpload(srcKey);
    long partSize = 5 << 20;
    int partCnt = (int) (fileSize / partSize + (fileSize % partSize == 0 ? 0 : 1));
    byte[] data =
        multipleUpload(srcMultipartUpload.key(), srcMultipartUpload.uploadId(), partCnt, true);
    MultipartUpload dstMultipartUpload = storage.createMultipartUpload(key);
    long copyPartRangeStart = 0L;
    List<Part> results = Lists.newArrayList();
    try {
      for (int i = 0; i < partCnt; i++) {
        Part result = storage.uploadPartCopy(srcKey, key, dstMultipartUpload.uploadId(), i + 1,
            copyPartRangeStart, Math.min(copyPartRangeStart + partSize, fileSize) - 1);
        results.add(result);
        copyPartRangeStart += partSize;
      }
      storage.completeUpload(key, dstMultipartUpload.uploadId(), results);
      assertArrayEquals(data, IOUtils.toByteArray(getStream(key)));
    } catch (Exception e) {
      storage.abortMultipartUpload(key, dstMultipartUpload.uploadId());
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testCopy0MB(ObjectStorage store) throws IOException {
    setEnv(store);
    String srcKey = "src0MB.txt";
    String dstKey = "dst0MB.txt";
    testCopy(srcKey, dstKey, 0);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testCopy5MB(ObjectStorage store) throws IOException {
    setEnv(store);
    String srcKey = "src5MB.txt";
    String dstKey = "dst5MB.txt";
    testCopy(srcKey, dstKey, 5 << 20);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testCopy10MB(ObjectStorage store) throws IOException {
    setEnv(store);
    String srcKey = "src10MB.txt";
    String dstKey = "dst10MB.txt";
    testCopy(srcKey, dstKey, 10 << 20);
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRename(ObjectStorage store) throws IOException {
    setEnv(store);
    String srcKey = "src.txt";
    String dstKey = "dst.txt";

    // Rename source to a un-exist object
    renameObject(srcKey, dstKey, 256);
    renameObject(srcKey, dstKey, 0);

    // Overwrite an existing object
    renameObjectWhenDestExist(srcKey, dstKey, 256, 0);
    renameObjectWhenDestExist(srcKey, dstKey, 0, 256);

    assertNull(storage.head(srcKey));
    assertThrows(RuntimeException.class, () -> storage.rename(srcKey, dstKey),
        "Source key not found");

    assertThrows(RuntimeException.class, () -> renameObject(srcKey, srcKey, 256),
        "Cannot rename to the same object");
  }

  private void renameObjectWhenDestExist(String srcKey, String dstKey, int srcSize, int destSize)
      throws IOException {
    byte[] dstData = new byte[destSize];
    storage.put(dstKey, dstData, 0, destSize);
    assertArrayEquals(dstData, IOUtils.toByteArray(getStream(dstKey)));

    renameObject(srcKey, dstKey, srcSize);
  }

  private void renameObject(String srcKey, String dstKey, int fileSize) throws IOException {
    byte[] data = new byte[fileSize];
    storage.put(srcKey, data, 0, fileSize);
    assertArrayEquals(data, IOUtils.toByteArray(getStream(srcKey)));

    storage.rename(srcKey, dstKey);
    assertArrayEquals(data, IOUtils.toByteArray(getStream(dstKey)));
    assertNull(storage.head(srcKey));

    storage.delete(dstKey);
    assertNull(storage.head(dstKey));
  }

  private void testCopy(String srcKey, String dstKey, int fileSize) throws IOException {
    byte[] data = new byte[fileSize];
    storage.put(srcKey, data, 0, fileSize);
    storage.copy(srcKey, dstKey);
    assertArrayEquals(data, IOUtils.toByteArray(getStream(dstKey)));
  }

  private ListObjectsResponse list(String prefix, String startAfter, int limit, String delimiter) {
    Preconditions.checkArgument(limit <= 1000, "Cannot list more than 1000 objects.");
    ListObjectsRequest request = ListObjectsRequest.builder()
        .prefix(prefix)
        .startAfter(startAfter)
        .maxKeys(limit)
        .delimiter(delimiter)
        .build();
    Iterator<ListObjectsResponse> iterator = storage.list(request).iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return new ListObjectsResponse(new ArrayList<>(), new ArrayList<>());
    }
  }

  private static <T> List<T> toList(final Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false)
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testObjectTagging(ObjectStorage store) {
    setEnv(store);
    assumeFalse(storage.bucket().isDirectory());
    if (storage instanceof FileStore) {
      return;
    }

    // create key.
    String key = "ObjectTagging";
    String tagPrefix = "tag" + UUIDUtils.random() + "_";
    String valuePrefix = "value" + UUIDUtils.random() + "_";
    storage.put(key, new byte[0], 0, 0);

    Map<String, String> tagsMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      tagsMap.put(tagPrefix + i, valuePrefix + i);
    }

    // 1. put and get when key exists.
    storage.putTags(key, tagsMap);
    Map<String, String> tags = storage.getTags(key);
    assertEquals(10, tags.keySet().size());
    assertTrue(Maps.difference(tagsMap, tags).areEqual());

    // 2. put and get when key doesn't exist.
    assertThrows(TosServerException.class, () -> storage.putTags("non-exist-key", tagsMap),
        "NoSuchKey");
    assertThrows(TosServerException.class, () -> storage.getTags("non-exist-key"), "doesn't exist");

    // 3. tag threshold.
    Map<String, String> bigMap = new HashMap<>(tagsMap);
    bigMap.put(tagPrefix + 11, valuePrefix + 11);
    assertThrows(RuntimeException.class, () -> storage.putTags(key, bigMap), "exceed limit of 10");

    // 4. put tag with null tagName.
    Map<String, String> nullKeyTag = new HashMap<>();
    nullKeyTag.put(null, "some value");
    assertThrows(TosServerException.class, () -> storage.putTags(key, nullKeyTag),
        "TagKey you have provided is invalid");

    // 5. put tag with null value.
    Map<String, String> nullValueTag = new HashMap<>();
    nullValueTag.put("some-key", null);
    storage.putTags(key, nullValueTag);
    assertNull(storage.getTags(key).get("some-key"));

    // 6. remove tags.
    Map<String, String> emptyTag = new HashMap<>();
    storage.putTags(key, emptyTag);
    assertEquals(0, storage.getTags(key).size());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testObjectChecksum(ObjectStorage store) throws IOException {
    setEnv(store);
    byte[] data = TestUtility.rand(256);
    String key = "a/truncated.txt";

    // Read object at the end offset.
    byte[] checksum = storage.put(key, () -> new ByteArrayInputStream(data), 200);
    ObjectContent objContent = storage.get(key, 200, -1);
    objContent.stream().close();
    assertArrayEquals(checksum, objContent.checksum());

    // Read empty object.
    checksum = storage.put(key, () -> new ByteArrayInputStream(new byte[0]), 0);
    objContent = storage.get(key, 0, -1);
    objContent.stream().close();
    assertArrayEquals(checksum, objContent.checksum());
  }
}
