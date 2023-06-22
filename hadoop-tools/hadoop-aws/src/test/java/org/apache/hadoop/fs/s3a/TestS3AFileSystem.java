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

package org.apache.hadoop.fs.s3a;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.regex.Pattern;

/**
 * Unit tests for {@link S3AFileSystem}.
 */
public class TestS3AFileSystem extends Assert {
  final File TEMP_DIR = new File("target/build/test/TestS3AFileSystem");
  final String longStr = // 1024 char
    "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key__very_long_s3_key__very_long_s3_key__very_long_s3_key__" +
      "very_long_s3_key";
  final String longStrTruncated = "very_long_s3_key__very_long_s3_key__";

  @Rule
  public Timeout testTimeout = new Timeout(30 * 1000);

  @Before
  public void init() throws IOException {
    Files.createDirectories(TEMP_DIR.toPath());
  }

  @After
  public void teardown() throws IOException {
    File[] testOutputFiles = TEMP_DIR.listFiles();
    for(File file: testOutputFiles) {
      Files.delete(file.toPath());
    }
    Files.deleteIfExists(TEMP_DIR.toPath());
  }

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Test the {@link S3AFileSystem#safeCreateTempFile(String, String, File)}.
   * The code verifies that the input prefix and suffix don't exceed the file system's max name
   * length and cause an exception.
   *
   * This test verifies the basic contract of the process.
   */
  @Test
  public void testSafeCreateTempFile() throws Throwable {
    // fitting name isn't changed
    File noChangesRequired = S3AFileSystem.safeCreateTempFile("noChangesRequired", ".tmp", TEMP_DIR);
    assertTrue(noChangesRequired.exists());
    String noChangesRequiredName = noChangesRequired.getName();
    assertTrue(noChangesRequiredName.startsWith("noChangesRequired"));
    assertTrue(noChangesRequiredName.endsWith(".tmp"));

    // a long prefix should be truncated
    File excessivelyLongPrefix = S3AFileSystem.safeCreateTempFile(longStr, ".tmp", TEMP_DIR);
    assertTrue(excessivelyLongPrefix.exists());
    String excessivelyLongPrefixName = excessivelyLongPrefix.getName();
    assertTrue(excessivelyLongPrefixName.startsWith(longStrTruncated));
    assertTrue(excessivelyLongPrefixName.endsWith(".tmp"));

    // a long suffix should be truncated
    File excessivelyLongSuffix = S3AFileSystem.safeCreateTempFile("excessivelyLongSuffix", "." + longStr, TEMP_DIR);
    assertTrue(excessivelyLongSuffix.exists());
    String excessivelyLongSuffixName = excessivelyLongSuffix.getName();
    // the prefix should have been truncated first
    assertTrue(excessivelyLongSuffixName.startsWith("exc"));
    Pattern p = Pattern.compile("^exc\\d{1,19}\\.very_long_s3_key__very_long_s3_key__");
    assertTrue(p.matcher(excessivelyLongSuffixName).find());
  }
}
