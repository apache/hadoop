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

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test setTimes -if supported
 */
public abstract class AbstractContractSetTimesTest extends
    AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractSetTimesTest.class);

  private Path testPath;
  private Path target;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_SETTIMES);

    //delete the test directory
    testPath = path("test");
    target = new Path(testPath, "target");
  }

  @Test
  public void testSetTimesNonexistentFile() throws Throwable {
    try {
      long time = System.currentTimeMillis();
      getFileSystem().setTimes(target, time, time);
      //got here: trouble
      fail("expected a failure");
    } catch (FileNotFoundException e) {
      //expected
      handleExpectedException(e);
    }
  }
}
