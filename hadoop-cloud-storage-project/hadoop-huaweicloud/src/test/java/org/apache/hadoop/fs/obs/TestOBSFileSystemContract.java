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

package org.apache.hadoop.fs.obs;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.junit.jupiter.api.BeforeEach;


/**
 * Tests a live OBS system. If your keys and bucket aren't specified, all tests
 * are marked as passed.
 * <p>
 * This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 * TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 * properly making it impossible to skip the tests if we don't have a valid
 * bucket.
 **/
public class TestOBSFileSystemContract extends FileSystemContractBaseTest {

  @BeforeEach
  public void setUp() throws Exception {
    skipTestCheck();
    Configuration conf = new Configuration();
    conf.addResource(OBSContract.CONTRACT_XML);
    fs = OBSTestUtils.createTestFileSystem(conf);
  }

  @Override
  public void testMkdirsWithUmask() {
    assumeTrue(false, "unspport.");
  }

  @Override
  public void testRenameRootDirForbidden() {
    assumeTrue(false, "unspport.");
  }

  public void skipTestCheck() {
    assumeTrue(OBSContract.isContractTestEnabled());
  }
}
