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

package org.apache.hadoop.fs.tosfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.tosfs.object.ObjectTestUtils.assertDirExist;
import static org.apache.hadoop.fs.tosfs.object.ObjectTestUtils.assertObjectNotExist;

public class TestDelete extends AbstractContractDeleteTest {

  @BeforeClass
  public static void before() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new TosContract(conf);
  }

  @Test
  public void testParentDirCreatedAfterDeleteSubChildren() throws IOException {
    Path path = path("testParentDirCreatedAfterDeleteSubChildren/");
    Path file1 = new Path(path, "f1");
    Path file2 = new Path(path, "f2");
    ContractTestUtils.writeTextFile(getFileSystem(), file1,
        "the first file", true);
    ContractTestUtils.writeTextFile(getFileSystem(), file2,
        "the second file", true);
    assertPathExists("file1 not created", file1);
    assertPathExists("file1 not created", file2);

    assertObjectNotExist(path, false);
    assertObjectNotExist(path, true);

    assertDeleted(file1, false);
    assertPathExists("parent path should exist", path);

    assertObjectNotExist(path, false);
    assertDirExist(path);
  }
}
