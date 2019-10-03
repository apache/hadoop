/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.Unit;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

/**
 * Contract test suit covering CosN integration with DistCp.
 */
public class TestCosNContractDistCp extends AbstractContractDistCpTest {

  private static final int MULTIPART_SETTING = 2 * Unit.MB;
  private static final long UPLOAD_BUFFER_POOL_SIZE = 5 * 2 * Unit.MB;
  private static final int UPLOAD_THREAD_POOL_SIZE = 5;
  private static final int COPY_THREAD_POOL_SIZE = 3;

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new CosNContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();
    newConf.setInt(CosNConfigKeys.COSN_BLOCK_SIZE_KEY,
        MULTIPART_SETTING);
    newConf.setLong(CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY,
        UPLOAD_BUFFER_POOL_SIZE);
    newConf.setInt(CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
        UPLOAD_THREAD_POOL_SIZE);
    newConf.setInt(CosNConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
        COPY_THREAD_POOL_SIZE);
    return newConf;
  }
}
