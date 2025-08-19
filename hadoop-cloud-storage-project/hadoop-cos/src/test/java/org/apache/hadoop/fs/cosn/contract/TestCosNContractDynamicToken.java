/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.cosn.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider;

import static org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider.STS_SECRET_ID_KEY;
import static org.apache.hadoop.fs.cosn.auth.DynamicTemporaryCosnCredentialsProvider.STS_SECRET_KEY_KEY;

/**
 * Contract tests for CosN using a dynamic temporary token provider (STS).
 * This test requires long-term credentials with STS access to be configured.
 */
public class TestCosNContractDynamicToken extends AbstractContractCreateTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new CosNContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();

    newConf.set(CosNConfigKeys.COSN_CREDENTIALS_PROVIDER,
        DynamicTemporaryCosnCredentialsProvider.class.getName());
    newConf.setBoolean("fs.cosn.impl.disable.cache", true);

    String stsSecretId = System.getProperty(STS_SECRET_ID_KEY);
    String stsSecretKey = System.getProperty(STS_SECRET_KEY_KEY);

    if (stsSecretId == null || stsSecretKey == null) {
      // Fallback to configuration for convenience, but log a warning.
      System.err.println("WARN: Reading long-term STS credentials from configuration file. "
          + "It is recommended to use system properties for security.");
      stsSecretId = newConf.get(STS_SECRET_ID_KEY);
      stsSecretKey = newConf.get(STS_SECRET_KEY_KEY);
    }

    if (stsSecretId == null || stsSecretKey == null) {
      throw new RuntimeException("STS credentials for tests are not provided. "
          + "Please set them via system properties (-Dfs.cosn.auth.sts.secret.id=... and "
          + "-Dfs.cosn.auth.sts.secret.key=...)");
    }

    newConf.set(STS_SECRET_ID_KEY, stsSecretId);
    newConf.set(STS_SECRET_KEY_KEY, stsSecretKey);

    return newConf;
  }
}