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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Tests for the {@link S3Guard} utility class.
 */
public class TestS3Guard extends Assert {

  /**
   * Basic test to ensure results from S3 and MetadataStore are merged
   * correctly.
   */
  @Test
  public void testDirListingUnion() throws Exception {
    MetadataStore ms = new LocalMetadataStore();

    Path dirPath = new Path("s3a://bucket/dir");

    // Two files in metadata store listing
    PathMetadata m1 = makePathMeta("s3a://bucket/dir/ms-file1", false);
    PathMetadata m2 = makePathMeta("s3a://bucket/dir/ms-file2", false);
    DirListingMetadata dirMeta = new DirListingMetadata(dirPath,
        Arrays.asList(m1, m2), false);

    // Two other files in s3
    List<FileStatus> s3Listing = Arrays.asList(
        makeFileStatus("s3a://bucket/dir/s3-file3", false),
        makeFileStatus("s3a://bucket/dir/s3-file4", false)
    );

    FileStatus[] result = S3Guard.dirListingUnion(ms, dirPath, s3Listing,
        dirMeta, false);

    assertEquals("listing length", 4, result.length);
    assertContainsPath(result, "s3a://bucket/dir/ms-file1");
    assertContainsPath(result, "s3a://bucket/dir/ms-file2");
    assertContainsPath(result, "s3a://bucket/dir/s3-file3");
    assertContainsPath(result, "s3a://bucket/dir/s3-file4");
  }

  void assertContainsPath(FileStatus[] statuses, String pathStr) {
    assertTrue("listing doesn't contain " + pathStr,
        containsPath(statuses, pathStr));
  }

  boolean containsPath(FileStatus[] statuses, String pathStr) {
    for (FileStatus s : statuses) {
      if (s.getPath().toString().equals(pathStr)) {
        return true;
      }
    }
    return false;
  }

  private PathMetadata makePathMeta(String pathStr, boolean isDir) {
    return new PathMetadata(makeFileStatus(pathStr, isDir));
  }

  private FileStatus makeFileStatus(String pathStr, boolean isDir) {
    Path p = new Path(pathStr);
    if (isDir) {
      return new FileStatus(0, true, 1, 1, System.currentTimeMillis(), p);
    } else {
      return new FileStatus(100, false, 1, 1, System.currentTimeMillis(), p);
    }
  }
}
