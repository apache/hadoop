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

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Zookeeper based router delegation token store implementation.
 */
public class ZKDelegationTokenSecretManagerImpl extends
    ZKDelegationTokenSecretManager<AbstractDelegationTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKDelegationTokenSecretManagerImpl.class);

  private Configuration conf = null;

  public ZKDelegationTokenSecretManagerImpl(Configuration conf) {
    super(conf);
    this.conf = conf;
    try {
      super.startThreads();
    } catch (IOException e) {
      LOG.error("Error starting threads for zkDelegationTokens", e);
    }
    LOG.info("Zookeeper delegation token secret manager instantiated");
  }

  @Override
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }
}
