/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKDelegationTokenFetcher extends DelegationTokenFetcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKDelegationTokenFetcher.class);

  protected static final String RM_DT_SECRET_MANAGER_ROOT = "RMDTSecretManagerRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";

  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME = "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME = "RMDTSequentialNumber";
  private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME = "RMDTMasterKeysRoot";
  public static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";

  private ZKCuratorManager zkManager;
  private String rootPath;

  public ZKDelegationTokenFetcher(Configuration conf, ZKCuratorManager zkcuratorManager,
      RouterDelegationTokenSecretManager secretManager) throws Exception {
    super(secretManager);
    this.zkManager = zkcuratorManager;
    this.rootPath = conf.get("yarn.resourcemanager.zk-state-store.rootpath", "/federation");
  }

  @Override
  public void start() throws Exception {

  }
}
