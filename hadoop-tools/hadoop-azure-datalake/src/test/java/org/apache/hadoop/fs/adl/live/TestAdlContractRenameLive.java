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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.LambdaTestUtils;

/**
 * Test rename contract test cases on Adl file system.
 */
public class TestAdlContractRenameLive extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration configuration) {
    return new AdlStorageContract(configuration);
  }

  /**
   * ADL throws an Access Control Exception rather than return false.
   * This is caught and its error text checked, to catch regressions.
   */
  @Test
  public void testRenameFileUnderFile() throws Exception {
    LambdaTestUtils.intercept(AccessControlException.class,
        "Parent path is not a folder.",
        super::testRenameFileUnderFile);
  }
}
