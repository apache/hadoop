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

package org.apache.hadoop.mapreduce.filecache;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.test.HadoopTestBase;

/**
 * Test how S3A resources are scoped in YARN caching.
 * In this package to make use of package-private methods of
 * {@link ClientDistributedCacheManager}.
 */
public class TestS3AResourceScope extends HadoopTestBase {

  private static final Path PATH = new Path("s3a://example/path");

  @Test
  public void testS3AFilesArePrivate() throws Throwable {
    S3AFileStatus status = new S3AFileStatus(false, PATH, "self");
    assertTrue("Not encrypted: " + status, status.isEncrypted());
    assertNotExecutable(status);
  }

  @Test
  public void testS3AFilesArePrivateOtherContstructor() throws Throwable {
    S3AFileStatus status = new S3AFileStatus(0, 0, PATH, 1, "self", null, null);
    assertTrue("Not encrypted: " + status, status.isEncrypted());
    assertNotExecutable(status);
  }

  private void assertNotExecutable(final S3AFileStatus status)
      throws IOException {
    Map<URI, FileStatus> cache = new HashMap<>();
    cache.put(PATH.toUri(), status);
    assertFalse("Should not have been executable " + status,
        ClientDistributedCacheManager.ancestorsHaveExecutePermissions(
            null, PATH, cache));
  }
}
