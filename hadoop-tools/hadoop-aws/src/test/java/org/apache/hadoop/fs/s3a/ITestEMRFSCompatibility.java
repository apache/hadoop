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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.S3N_FOLDER_SUFFIX;

/**
 * This test verifies that the EMRFS or legacy S3N filesystem compatibility with
 * S3A works as expected.
 */
public class ITestEMRFSCompatibility extends AbstractS3ATestBase {
  private static final String SRC_DIR = "src";
  private static final String DEST_DIR = "dest";
  private static final String SUBDIR = "subdir";

  @Test
  public void testFileSystemOperationWithS3NFolderMarker() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path basePath = methodPath();
    Path src = new Path(basePath, SRC_DIR);
    Path dest = new Path(basePath, DEST_DIR);
    Path subdir = new Path(src, SUBDIR);
    Path folderMarker = new Path(subdir, S3N_FOLDER_SUFFIX);

    // write an empty S3N folder marker object
    touch(fs, folderMarker);

    // verify initial state
    Assertions.assertThat(fs.listStatus(subdir))
        .describedAs("No objects are expected to be listed")
        .isEmpty();

    // rename the src folder.
    fs.rename(src, dest);

    //verify destination folder exists
    Assertions.assertThat(fs.exists(new Path(dest, "subdir")))
        .describedAs("Destination folder should exist")
        .isTrue();

    // verify src folder does not exists
    Assertions.assertThat(fs.exists(src))
        .describedAs("Source folder should not exist after rename")
        .isFalse();
  }
}
