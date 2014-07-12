/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.fs.contract.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractAppendTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestHDFSContractAppend extends AbstractContractAppendTest {

  @BeforeClass
  public static void createCluster() throws IOException {
    HDFSContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    HDFSContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new HDFSContract(conf);
  }

  @Override
  public void testRenameFileBeingAppended() throws Throwable {
    try {
      super.testRenameFileBeingAppended();
      fail("Expected a FileNotFoundException");
    } catch (FileNotFoundException e) {
      // downgrade
      ContractTestUtils.downgrade("Renaming an open file" +
                                  "still creates the old path", e);

    }
  }
}
