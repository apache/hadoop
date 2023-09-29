/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractContentSummaryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

public class ITestS3AContractContentSummary extends AbstractContractContentSummaryTest {

  @Test
  public void testGetContentSummaryDir() throws Throwable {
    describe("getContentSummary on test dir with children");
    S3AFileSystem fs = getFileSystem();
    Path baseDir = methodPath();

    // Nested folders created separately will return as separate objects in listFiles()
    fs.mkdirs(new Path(baseDir, "a"));
    fs.mkdirs(new Path(baseDir, "a/b"));
    fs.mkdirs(new Path(baseDir, "a/b/a"));

    // Will return as one object
    fs.mkdirs(new Path(baseDir, "d/e/f"));

    Path filePath = new Path(baseDir, "a/b/file");
    touch(fs, filePath);

    // look at path to see if it is a file
    // it is not: so LIST
    final ContentSummary summary = fs.getContentSummary(baseDir);

    Assertions.assertThat(summary.getDirectoryCount()).as("Summary " + summary).isEqualTo(7);
    Assertions.assertThat(summary.getFileCount()).as("Summary " + summary).isEqualTo(1);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

}
