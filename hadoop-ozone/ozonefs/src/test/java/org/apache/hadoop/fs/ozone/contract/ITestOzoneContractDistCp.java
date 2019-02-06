/**
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

package org.apache.hadoop.fs.ozone.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;


/**
 * Contract test suite covering S3A integration with DistCp.
 * Uses the block output stream, buffered to disk. This is the
 * recommended output mechanism for DistCP due to its scalability.
 */
public class ITestOzoneContractDistCp extends AbstractContractDistCpTest {

  @BeforeClass
  public static void createCluster() throws IOException {
    OzoneContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    OzoneContract.destroyCluster();
  }

  @Override
  protected OzoneContract createContract(Configuration conf) {
    return new OzoneContract(conf);
  }
}
