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

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public abstract class AbstractContractContentSummaryTest extends AbstractFSContractTestBase {

  @Test
  public void testGetContentSummary() throws Throwable {
    FileSystem fs = getFileSystem();

    Path parent = path("parent");
    Path nested = path(parent + "/a/b/c");
    Path filePath = path(nested + "file.txt");

    fs.mkdirs(parent);
    fs.mkdirs(nested);
    touch(getFileSystem(), filePath);

    ContentSummary summary = fs.getContentSummary(parent);

    Assertions.assertThat(summary.getDirectoryCount()).as("Summary " + summary).isEqualTo(4);

    Assertions.assertThat(summary.getFileCount()).as("Summary " + summary).isEqualTo(1);
  }

  @Test
  public void testGetContentSummaryIncorrectPath() throws Throwable {
    FileSystem fs = getFileSystem();

    Path parent = path("parent");
    Path nested = path(parent + "/a");

    fs.mkdirs(parent);

    intercept(FileNotFoundException.class, () -> fs.getContentSummary(nested));
  }
}
