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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

/**
 * The contract of OBS: only enabled if the test bucket is provided.
 */
public class OBSContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "contract/obs.xml";

  private static final String CONTRACT_ENABLE_KEY =
      "fs.obs.test.contract.enable";

  private static final boolean CONTRACT_ENABLE_DEFAULT = false;

  public OBSContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "obs";
  }

  @Override
  public Path getTestPath() {
    return OBSTestUtils.createTestPath(super.getTestPath());
  }

  public synchronized static boolean isContractTestEnabled() {
    Configuration conf = null;
    boolean isContractTestEnabled = true;

    if (conf == null) {
      conf = getConfiguration();
    }
    String fileSystem = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      isContractTestEnabled = false;
    }
    return isContractTestEnabled;
  }

  public synchronized static Configuration getConfiguration() {
    Configuration newConf = new Configuration();
    newConf.addResource(CONTRACT_XML);
    return newConf;
  }
}
