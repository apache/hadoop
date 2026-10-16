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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestStringPaths {
  @Test
  public void testValidateBucketNameValid() {
    assertEquals("my-bucket", StringPaths.validateBucketName("my-bucket"));
    assertEquals("my.bucket", StringPaths.validateBucketName("my.bucket"));
    assertEquals("my_bucket", StringPaths.validateBucketName("my_bucket"));
    assertEquals("bucket123", StringPaths.validateBucketName("bucket123"));
    assertEquals("a", StringPaths.validateBucketName("a"));
    assertEquals("long-bucket-name-with-numbers-123",
        StringPaths.validateBucketName("long-bucket-name-with-numbers-123"));
  }

  @Test
  public void testValidateBucketNameEndsWithSlash() {
    assertEquals("my-bucket", StringPaths.validateBucketName("my-bucket/"));
    assertEquals("another-bucket", StringPaths.validateBucketName("another-bucket/"));
  }

  @Test
  public void testValidateBucketNameEmpty() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateBucketName("");
    });
  }

  @Test
  public void testValidateBucketNameNull() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateBucketName(null);
    });
  }

  @Test
  public void testValidateBucketNameInvalidChars() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateBucketName("my bucket"); // Space
    });
  }

  @Test
  public void testValidateBucketNameInvalidChars2() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateBucketName("my@bucket"); // @ symbol
    });
  }

  @Test
  public void testValidateBucketNameUpperCase() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateBucketName("MyBucket"); // Uppercase
    });
  }

  @Test
  public void testValidateObjectNameValid() {
    assertEquals("path/to/object",
        StringPaths.validateObjectName("path/to/object", false));
    assertEquals("object", StringPaths.validateObjectName("object", false));
    assertEquals("dir/",
        StringPaths.validateObjectName("dir/", false)); // Still valid after validation
    assertEquals("", StringPaths.validateObjectName("/", true)); // Slash becomes empty if allowed
    assertEquals("", StringPaths.validateObjectName("", true));
  }

  @Test
  public void testValidateObjectNameLeadingSlash() {
    assertEquals("path/to/object", StringPaths.validateObjectName("/path/to/object", false));
    assertEquals("object", StringPaths.validateObjectName("/object", false));
  }

  @Test
  public void testValidateObjectNameEmptyNotAllowed() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateObjectName("", false);
    });
  }

  @Test
  public void testValidateObjectNameNullNotAllowed() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateObjectName(null, false);
    });
  }

  @Test
  public void testValidateObjectNameEmptyAllowed() {
    assertEquals("", StringPaths.validateObjectName("", true));
    assertEquals("", StringPaths.validateObjectName(null, true));
    assertEquals("", StringPaths.validateObjectName("/", true)); // Single slash becomes empty
  }

  @Test
  public void testValidateObjectNameConsecutiveSlashes() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateObjectName("path//to/object", false);
    });
  }

  @Test
  public void testValidateObjectNameConsecutiveSlashesAtStart() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateObjectName("//path/to/object", false);
    });
  }

  @Test
  public void testValidateObjectNameConsecutiveSlashesAtEnd() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.validateObjectName("path/to/object//", false);
    });
  }

  @Test
  public void testFromComponentsValid() {
    assertEquals("gs://my-bucket/path/to/object",
        StringPaths.fromComponents("my-bucket", "path/to/object"));
    assertEquals("gs://my-bucket/dir/", StringPaths.fromComponents("my-bucket", "dir/"));
    assertEquals("gs://my-bucket/", StringPaths.fromComponents("my-bucket", ""));
  }

  @Test
  public void testFromComponentsNullBucketNonNullObject() {
    assertThrows(IllegalArgumentException.class, () -> {
      StringPaths.fromComponents(null, "path/to/object");
    });
  }

  @Test
  public void testFromComponentsNullBucketAndObject() {
    assertEquals("gs://", StringPaths.fromComponents(null, null));
  }

  @Test
  public void testIsDirectoryPath() {
    assertTrue(StringPaths.isDirectoryPath("dir/"));
    assertTrue(StringPaths.isDirectoryPath("path/to/dir/"));
    assertFalse(StringPaths.isDirectoryPath("file.txt"));
    assertFalse(StringPaths.isDirectoryPath("path/to/file.txt"));
    assertFalse(StringPaths.isDirectoryPath(""));
    assertFalse(StringPaths.isDirectoryPath(null));
  }

  @Test
  public void testToFilePath() {
    assertEquals("path/to/file", StringPaths.toFilePath("path/to/file/"));
    assertEquals("file.txt", StringPaths.toFilePath("file.txt"));
    assertEquals("dir", StringPaths.toFilePath("dir/"));
    assertEquals("", StringPaths.toFilePath(""));
    assertNull(StringPaths.toFilePath(null));
  }

  // --- Tests for toDirectoryPath ---

  @Test
  public void testToDirectoryPath() {
    assertEquals("path/to/dir/", StringPaths.toDirectoryPath("path/to/dir"));
    assertEquals("dir/", StringPaths.toDirectoryPath("dir/"));
    assertEquals("file/", StringPaths.toDirectoryPath("file"));
    assertEquals("", StringPaths.toDirectoryPath(""));
    assertNull(StringPaths.toDirectoryPath(null));
  }
}
