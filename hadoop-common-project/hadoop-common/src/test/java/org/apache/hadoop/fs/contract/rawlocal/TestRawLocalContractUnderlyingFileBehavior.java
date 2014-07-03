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

package org.apache.hadoop.fs.contract.rawlocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class TestRawLocalContractUnderlyingFileBehavior extends Assert {

  private static File testDirectory;

  @BeforeClass
  public static void before() {
    RawlocalFSContract contract =
      new RawlocalFSContract(new Configuration());
    testDirectory = contract.getTestDirectory();
    testDirectory.mkdirs();
    assertTrue(testDirectory.isDirectory());

  }

  @Test
  public void testDeleteEmptyPath() throws Throwable {
    File nonexistent = new File(testDirectory, "testDeleteEmptyPath");
    assertFalse(nonexistent.exists());
    assertFalse("nonexistent.delete() returned true", nonexistent.delete());
  }
}
