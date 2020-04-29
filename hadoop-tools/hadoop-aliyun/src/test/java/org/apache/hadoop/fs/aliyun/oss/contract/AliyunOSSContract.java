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

package org.apache.hadoop.fs.aliyun.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

import java.io.IOException;

/**
 * The contract of Aliyun OSS: only enabled if the test bucket is provided.
 */
public class AliyunOSSContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "contract/aliyun-oss.xml";

  public AliyunOSSContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "oss";
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return AliyunOSSTestUtils.createTestFileSystem(new Configuration());
  }

  @Override
  public Path getTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? super.getTestPath() :
        new Path("/" + testUniqueForkId, "test");
  }
}
