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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRawFileSystem {
  @Test
  public void testInitializeFileSystem() throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    try (RawFileSystem fs = new RawFileSystem()) {
      fs.initialize(new URI("filestore://bucket_a/a/b/c"), conf);
      assertEquals("bucket_a", fs.bucket());

      fs.initialize(new URI("filestore://bucket-/a/b/c"), conf);
      assertEquals("bucket-", fs.bucket());

      fs.initialize(new URI("filestore://-bucket/a/b/c"), conf);
      assertEquals("-bucket", fs.bucket());
    }
  }

  @Test
  public void testBucketNotExist() {
    Configuration conf = new Configuration();
    RawFileSystem fs = new RawFileSystem();
    assertThrows(FileNotFoundException.class,
        () -> fs.initialize(new URI("tos://not-exist-bucket/"), conf), "Bucket doesn't exist.");
  }
}
