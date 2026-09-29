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
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRawFileSystem {
  private static final String FILE_STORE_ROOT = TempFiles.newTempDir("TestTosChecksum");

  @Test
  public void testInitializeFileSystem() throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("filestore"), FILE_STORE_ROOT);
    try (RawFileSystem fs = new RawFileSystem()) {
      fs.initialize(new URI("filestore://bucket_a/a/b/c"), conf);
      assertEquals("bucket_a", fs.bucket());

      fs.initialize(new URI("filestore://bucket-/a/b/c"), conf);
      assertEquals("bucket-", fs.bucket());

      fs.initialize(new URI("filestore://-bucket/a/b/c"), conf);
      assertEquals("-bucket", fs.bucket());
    }
  }
}
