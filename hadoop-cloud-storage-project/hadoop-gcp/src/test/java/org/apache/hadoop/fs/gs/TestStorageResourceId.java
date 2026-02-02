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

package org.apache.hadoop.fs.gs;

import java.net.URI;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestStorageResourceId {
  @Test
  public void testConstructorInvalid() {
    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId(null);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("");
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId(null, null);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("foo", null);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("", null);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId(null, null, 0L);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("foo", null, 0L);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("", null, 0L);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId(null, 0L);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      new StorageResourceId("", 0L);
    });
  }

  @Test
  public void testFromStringPathInvalid() {
    assertThrows(IllegalArgumentException.class, () -> {
      StorageResourceId.fromStringPath(null);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      StorageResourceId.fromStringPath("");
    });

    assertThrows(IllegalArgumentException.class, () -> {
      StorageResourceId.fromStringPath("foo");
    });

    assertThrows(IllegalArgumentException.class, () -> {
      StorageResourceId.fromStringPath("/foo/bar");
    });

    assertThrows(IllegalArgumentException.class, () -> {
      StorageResourceId.fromStringPath("gs:///foo/bar");
    });
  }

  @Test
  public void testConstructor() {
    String bucketName = "testbucketname";
    String objectName = "a/b/c.txt";

    verify(new StorageResourceId(bucketName), bucketName,
        StorageResourceId.UNKNOWN_GENERATION_ID, null, false,
        true, true, false, false);

    verify(new StorageResourceId(bucketName, objectName), bucketName,
        StorageResourceId.UNKNOWN_GENERATION_ID, objectName, false,
        false, false, true, false);

    long genId = System.currentTimeMillis();
    verify(new StorageResourceId(bucketName, objectName, genId), bucketName,
        genId, objectName, true,
        false, false, true, false);

    verify(new StorageResourceId(bucketName, genId), bucketName,
        genId, null, true,
        true, true, false, false);
  }

  @Test
  public void testEqualsBucket() {
    StorageResourceId bucket1 = new StorageResourceId("test-bucket");
    StorageResourceId bucket2 = new StorageResourceId("test-bucket");
    assertTrue(bucket1.equals(bucket2));
    assertEquals(bucket1.hashCode(), bucket2.hashCode());
  }

  @Test
  public void testEqualsObject() {
    StorageResourceId obj1 = new StorageResourceId("test-bucket", "test-object");
    StorageResourceId obj2 = new StorageResourceId("test-bucket", "test-object");
    assertTrue(obj1.equals(obj2));
    assertEquals(obj1.hashCode(), obj2.hashCode());
  }

  @Test
  public void testEqualsDifferentBucket() {
    StorageResourceId bucket1 = new StorageResourceId("test-bucket");
    StorageResourceId bucket2 = new StorageResourceId("other-bucket");
    assertFalse(bucket1.equals(bucket2));
  }

  @Test
  public void testEqualsDifferentObject() {
    StorageResourceId obj1 = new StorageResourceId("test-bucket", "test-object");
    StorageResourceId obj2 = new StorageResourceId("test-bucket", "other-object");
    assertFalse(obj1.equals(obj2));
  }

  @Test
  public void testToDirectoryIdFromFile() {
    StorageResourceId fileId = new StorageResourceId("my-bucket", "path/to/file.txt");
    StorageResourceId dirId = fileId.toDirectoryId();

    assertNotSame(fileId, dirId); // Should return a new instance
    assertTrue(dirId.isDirectory());
    assertEquals("my-bucket", dirId.getBucketName());
    assertEquals("path/to/file.txt/", dirId.getObjectName());
    assertEquals(fileId.getGenerationId(), dirId.getGenerationId());
  }

  @Test
  public void testToDirectoryIdFromDirectoryObject() {
    StorageResourceId dirIdOriginal = new StorageResourceId("my-bucket", "path/to/dir/");
    StorageResourceId dirIdConverted = dirIdOriginal.toDirectoryId();

    assertSame(dirIdOriginal, dirIdConverted); // Should return the same instance
    assertTrue(dirIdConverted.isDirectory());
    assertEquals("path/to/dir/", dirIdConverted.getObjectName());
  }

  @Test
  public void testToDirectoryIdFromBucket() {
    StorageResourceId bucketId = new StorageResourceId("my-bucket");
    StorageResourceId convertedId = bucketId.toDirectoryId();
    assertSame(bucketId, convertedId);
    assertTrue(convertedId.isBucket());
  }

  @Test
  public void testFromStringPathRoot() {
    StorageResourceId id = StorageResourceId.fromStringPath("gs://");
    assertTrue(id.isRoot());
  }

  @Test
  public void testFromStringPathBucket() {
    StorageResourceId id = StorageResourceId.fromStringPath("gs://my-bucket");
    assertTrue(id.isBucket());
    assertEquals("my-bucket", id.getBucketName());
    assertNull(id.getObjectName());
    assertEquals(StorageResourceId.UNKNOWN_GENERATION_ID, id.getGenerationId());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "gs://my-bucket/object",
      "gs://my-bucket/folder/file.txt",
      "gs://my-bucket/folder/"
  })
  public void testFromStringPathObject(String path) {
    String expectedBucket = path.split("/")[2];
    String expectedObject =
        path.substring(path.indexOf(expectedBucket) + expectedBucket.length() + 1);

    StorageResourceId id = StorageResourceId.fromStringPath(path);
    assertTrue(id.isStorageObject());
    assertEquals(expectedBucket, id.getBucketName());
    assertEquals(expectedObject, id.getObjectName());
    assertEquals(StorageResourceId.UNKNOWN_GENERATION_ID, id.getGenerationId());
  }

  @Test
  public void testFromStringPathObjectWithGenerationId() {
    long genId = 12345L;
    StorageResourceId id = StorageResourceId.fromStringPath("gs://my-bucket/object.txt", genId);
    assertTrue(id.isStorageObject());
    assertEquals("my-bucket", id.getBucketName());
    assertEquals("object.txt", id.getObjectName());
    assertEquals(genId, id.getGenerationId());
    assertTrue(id.hasGenerationId());
  }

  @Test
  public void testFromUriPathBucket() throws Exception {
    URI uri = new URI("gs://my-bucket");
    StorageResourceId id = StorageResourceId.fromUriPath(uri, true);
    assertTrue(id.isBucket());
    assertEquals("my-bucket", id.getBucketName());
    assertNull(id.getObjectName());
  }

  @Test
  public void testFromUriPathObject() throws Exception {
    URI uri = new URI("gs://my-bucket/path/to/file.txt");
    StorageResourceId id = StorageResourceId.fromUriPath(uri, false);
    assertTrue(id.isStorageObject());
    assertEquals("my-bucket", id.getBucketName());
    assertEquals("path/to/file.txt", id.getObjectName());
  }

  @Test
  public void testFromUriPathObjectWithGenerationId() throws Exception {
    URI uri = new URI("gs://my-bucket/object.txt");
    long genId = 54321L;
    StorageResourceId id = StorageResourceId.fromUriPath(uri, false, genId);
    assertTrue(id.isStorageObject());
    assertEquals("my-bucket", id.getBucketName());
    assertEquals("object.txt", id.getObjectName());
    assertEquals(genId, id.getGenerationId());
    assertTrue(id.hasGenerationId());
  }

  @Test
  public void testFromUriPathBucketWithGenerationId() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> {
      URI uri = new URI("gs://my-bucket");
      long genId = 54321L;
      StorageResourceId.fromUriPath(uri, false, genId);
    });
  }

  private static void verify(
      StorageResourceId id,
      String bucketName,
      long generationId,
      String objectName,
      boolean hasGenerationId,
      boolean isBucket,
      boolean isDirectory,
      boolean isStorageObject,
      boolean isRoot) {
    assertEquals(bucketName, id.getBucketName());
    assertEquals(generationId, id.getGenerationId());
    assertEquals(objectName, id.getObjectName());
    assertEquals(hasGenerationId, id.hasGenerationId());
    assertEquals(isBucket, id.isBucket());
    assertEquals(isDirectory, id.isDirectory());
    assertEquals(isStorageObject, id.isStorageObject());
    assertEquals(isRoot, id.isRoot());
  }
}
