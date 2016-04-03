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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test getFileStatus -if supported
 */
public abstract class AbstractContractGetFileStatusTest extends
    AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractGetFileStatusTest.class);

  private Path testPath;
  private Path target;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_GETFILESTATUS);

    //delete the test directory
    testPath = path("test");
    target = new Path(testPath, "target");
  }

  @Test
  public void testGetFileStatusNonexistentFile() throws Throwable {
    try {
      FileStatus status = getFileSystem().getFileStatus(target);
      //got here: trouble
      fail("expected a failure");
    } catch (FileNotFoundException e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testListStatusEmptyDirectory() throws IOException {
    // remove the test directory
    FileSystem fs = getFileSystem();
    assertTrue(fs.delete(getContract().getTestPath(), true));

    // create a - non-qualified - Path for a subdir
    Path subfolder = getContract().getTestPath().suffix("/"+testPath.getName());
    assertTrue(fs.mkdirs(subfolder));

    // assert empty ls on the empty dir
    assertEquals("ls on an empty directory not of length 0", 0,
        fs.listStatus(subfolder).length);

    // assert non-empty ls on parent dir
    assertTrue("ls on a non-empty directory of length 0",
        fs.listStatus(getContract().getTestPath()).length > 0);
  }
}
