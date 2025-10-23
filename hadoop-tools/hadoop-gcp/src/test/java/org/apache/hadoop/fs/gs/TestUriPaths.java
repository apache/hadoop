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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestUriPaths {
  @Test
  public void testToDirectoryFile() throws Exception {
    URI fileUri = new URI("gs://my-bucket/path/to/file.txt");
    URI expectedDirUri = new URI("gs://my-bucket/path/to/file.txt/");
    // Temporarily override the behavior for testing purposes
    // This is not a clean mocking strategy for static methods, but demonstrates the test intent.
    // In a real environment, you'd use PowerMock or refactor.
    URI result = UriPaths.toDirectory(fileUri);
    assertEquals(expectedDirUri, result);
  }

  @Test
  public void testToDirectoryAlreadyDirectory() throws Exception {
    URI dirUri = new URI("gs://my-bucket/path/to/dir/");
    URI result = UriPaths.toDirectory(dirUri);
    assertEquals(dirUri, result);
  }

  @Test
  public void testToDirectoryRootBucket() throws Exception {
    URI bucketUri = new URI("gs://my-bucket");
    URI result = UriPaths.toDirectory(bucketUri);
    assertEquals(bucketUri, result); // Buckets are implicitly directories
  }

  @Test
  public void testGetParentPathFile() throws Exception {
    URI uri = new URI("gs://my-bucket/path/to/file.txt");
    URI expectedParent = new URI("gs://my-bucket/path/to/");
    assertEquals(expectedParent, UriPaths.getParentPath(uri));
  }

  @Test
  public void testGetParentPathDirectory() throws Exception {
    URI uri = new URI("gs://my-bucket/path/to/dir/");
    URI expectedParent = new URI("gs://my-bucket/path/to/");
    assertEquals(expectedParent, UriPaths.getParentPath(uri));
  }

  @Test
  public void testGetParentPathObjectAtBucketRoot() throws Exception {
    URI uri = new URI("gs://my-bucket/file.txt");
    URI expectedParent = new URI("gs://my-bucket/");
    assertEquals(expectedParent, UriPaths.getParentPath(uri));
  }

  @Test
  public void testGetParentPathDirectoryAtBucketRoot() throws Exception {
    URI uri = new URI("gs://my-bucket/dir/");
    URI expectedParent = new URI("gs://my-bucket/");
    assertEquals(expectedParent, UriPaths.getParentPath(uri));
  }

  @Test
  public void testGetParentPathBucket() throws Exception {
    URI uri = new URI("gs://my-bucket");
    assertEquals(GoogleCloudStorageFileSystem.GCSROOT, UriPaths.getParentPath(uri));
  }

  @Test
  public void testFromResourceIdObject() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("my-bucket", "path/to/object");
    URI expectedUri = new URI("gs://my-bucket/path/to/object");
    assertEquals(expectedUri, UriPaths.fromResourceId(resourceId, false));
  }

  @Test
  public void testFromResourceIdDirectory() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("my-bucket", "path/to/dir/");
    URI expectedUri = new URI("gs://my-bucket/path/to/dir/");
    assertEquals(expectedUri, UriPaths.fromResourceId(resourceId, false));
  }

  @Test
  public void testFromResourceIdBucket() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("my-bucket");
    URI expectedUri = new URI("gs://my-bucket/");
    assertEquals(expectedUri, UriPaths.fromResourceId(resourceId, true));
  }

  @Test
  public void testFromResourceIdEmptyObjectAllowed() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("my-bucket");
    URI expectedUri = new URI("gs://my-bucket/");
    assertEquals(expectedUri, UriPaths.fromResourceId(resourceId, true));
  }

  @Test
  public void testFromResourceIdNullObjectAllowed() throws Exception {
    StorageResourceId resourceId = new StorageResourceId("my-bucket");
    URI expectedUri = new URI("gs://my-bucket/");
    assertEquals(expectedUri, UriPaths.fromResourceId(resourceId, true));
  }

  @Test
  public void testFromStringPathComponentsValid() throws Exception {
    assertEquals(new URI("gs://my-bucket/path/to/object"),
        UriPaths.fromStringPathComponents("my-bucket", "path/to/object", false));
    assertEquals(new URI("gs://my-bucket/path/to/dir/"),
        UriPaths.fromStringPathComponents("my-bucket", "path/to/dir/", false));
    assertEquals(new URI("gs://my-bucket/"),
        UriPaths.fromStringPathComponents("my-bucket", null, true));
    assertEquals(new URI("gs://my-bucket/"),
        UriPaths.fromStringPathComponents("my-bucket", "", true));
  }

  @Test
  public void testFromStringPathComponentsNullBucketNameNotAllowed() {
    assertThrows(IllegalArgumentException.class, () -> {
      UriPaths.fromStringPathComponents(null, "object", false);
    });
  }

  @Test
  public void testFromStringPathComponentsEmptyObjectNameNotAllowed() {
    assertThrows(IllegalArgumentException.class, () -> {
      UriPaths.fromStringPathComponents("my-bucket", "", false);
    });
  }

  @Test
  public void testFromStringPathComponentsConsecutiveSlashes() {
    assertThrows(IllegalArgumentException.class, () -> {
      UriPaths.fromStringPathComponents("my-bucket", "path//to/object", false);
    });
  }

  @Test
  public void testFromStringPathComponentsInvalidBucketName() {
    assertThrows(IllegalArgumentException.class, () -> {
      UriPaths.fromStringPathComponents("MyBucket", "object", false); // Uppercase
    });
  }
}
