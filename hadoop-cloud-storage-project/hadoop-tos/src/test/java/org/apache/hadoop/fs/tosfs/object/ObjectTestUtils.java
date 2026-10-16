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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ObjectTestUtils {

  public static final byte[] EMPTY_BYTES = new byte[] {};

  private ObjectTestUtils() {
  }

  /**
   * Assert that all the parent directories should be existing.
   *
   * @param path to validate, can be directory or file.
   */
  public static void assertParentDirExist(Path path) throws IOException {
    for (Path p = path.getParent(); p != null && p.getParent() != null; p = p.getParent()) {
      assertObject(p, EMPTY_BYTES, true);
    }
  }

  /**
   * Assert that all the parent directories and current directory should be existing.
   *
   * @param path to validate, must be a directory.
   */
  public static void assertDirExist(Path path) throws IOException {
    // All parent directories exist.
    assertParentDirExist(path);
    // The current directory exist.
    assertObject(path, EMPTY_BYTES, true);
  }

  public static void assertObjectNotExist(Path path) throws IOException {
    assertObjectNotExist(path, false);
  }

  public static void assertObjectNotExist(Path path, boolean isDir) throws IOException {
    ObjectStorage store =
        ObjectStorageFactory.create(path.toUri().getScheme(), path.toUri().getHost(),
            new Configuration());
    String objectKey = ObjectUtils.pathToKey(path, isDir);
    ObjectInfo info = store.head(objectKey);
    assertNull(info, String.format("Object key %s shouldn't exist in backend storage.", objectKey));

    store.close();
  }

  public static void assertObject(Path path, byte[] data) throws IOException {
    assertObject(path, data, false);
  }

  public static void assertObject(Path path, byte[] data, boolean isDir) throws IOException {
    ObjectStorage store =
        ObjectStorageFactory.create(path.toUri().getScheme(), path.toUri().getHost(),
            new Configuration());
    String objectKey = ObjectUtils.pathToKey(path, isDir);
    // Verify the existence of object.
    ObjectInfo info = store.head(objectKey);
    assertNotNull(info, String.format("there should be an key %s in object storage", objectKey));
    assertEquals(info.key(), objectKey);
    assertEquals(data.length, info.size());
    // Verify the data content.
    try (InputStream in = store.get(objectKey, 0, -1).stream()) {
      byte[] actual = IOUtils.toByteArray(in);
      assertArrayEquals(data, actual, "Unexpected binary");
    }

    store.close();
  }

  public static void assertMultipartUploadExist(Path path, String uploadId) throws IOException {
    ObjectStorage store =
        ObjectStorageFactory.create(path.toUri().getScheme(), path.toUri().getHost(),
            new Configuration());
    String objectKey = ObjectUtils.pathToKey(path, false);

    Iterator<MultipartUpload> uploadIterator = store.listUploads(objectKey).iterator();
    assertTrue(uploadIterator.hasNext());
    assertMultipartUploadIdExist(uploadIterator, uploadId);

    store.close();
  }

  private static void assertMultipartUploadIdExist(Iterator<MultipartUpload> uploadIterator,
      String uploadId) {
    boolean exist = false;
    while (uploadIterator.hasNext()) {
      if (Objects.equals(uploadIterator.next().uploadId(), uploadId)) {
        exist = true;
      }
    }
    assertTrue(exist);
  }
}
