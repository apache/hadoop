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
package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import org.junit.BeforeClass;

/**
 * End-to-end tests for COMPOSITE_CRC combine mode.
 */
public class TestCopyMapperCompositeCrc extends TestCopyMapper {
  @BeforeClass
  public static void setup() throws Exception {
    Configuration configuration = TestCopyMapper.getConfigurationForCluster();
    configuration.set(
        HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_KEY, "COMPOSITE_CRC");
    TestCopyMapper.setCluster(new MiniDFSCluster.Builder(configuration)
                .numDataNodes(1)
                .format(true)
                .build());
  }

  @Override
  protected boolean expectDifferentBlockSizesMultipleBlocksToSucceed() {
    return true;
  }

  @Override
  protected boolean expectDifferentBytesPerCrcToSucceed() {
    return true;
  }
}
