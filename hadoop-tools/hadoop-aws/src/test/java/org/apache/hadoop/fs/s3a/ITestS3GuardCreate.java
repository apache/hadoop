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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Home for testing the creation of new files and directories with S3Guard
 * enabled.
 */
public class ITestS3GuardCreate extends AbstractS3ATestBase {

  /**
   * Test that ancestor creation during S3AFileSystem#create() is properly
   * accounted for in the MetadataStore.  This should be handled by the
   * FileSystem, and be a FS contract test, but S3A does not handle ancestors on
   * create(), so we need to take care in the S3Guard code to do the right
   * thing.  This may change: See HADOOP-13221 for more detail.
   */
  @Test
  public void testCreatePopulatesFileAncestors() throws Exception {
    final S3AFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.hasMetadataStore());
    final MetadataStore ms = fs.getMetadataStore();
    final Path parent = path("testCreatePopulatesFileAncestors");

    try {
      fs.mkdirs(parent);
      final Path nestedFile = new Path(parent, "dir1/dir2/file4");
      touch(fs, nestedFile);

      DirListingMetadata list = ms.listChildren(parent);
      assertFalse("MetadataStore falsely reports authoritative empty list",
          list.isEmpty() == Tristate.TRUE);
    } finally {
      fs.delete(parent, true);
    }
  }
}
