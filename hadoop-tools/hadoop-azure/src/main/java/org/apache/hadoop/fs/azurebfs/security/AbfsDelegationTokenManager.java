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


package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.CustomDelegationTokenManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Class for delegation token Manager.
 */
public class AbfsDelegationTokenManager {

  private CustomDelegationTokenManager tokenManager;
  private static final Logger LOG =
          LoggerFactory.getLogger(AbfsDelegationTokenManager.class);

  public AbfsDelegationTokenManager(final Configuration conf) throws IOException {

    Preconditions.checkNotNull(conf, "conf");

    Class<? extends CustomDelegationTokenManager> customDelegationTokenMgrClass =
            conf.getClass(ConfigurationKeys.FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE, null,
                    CustomDelegationTokenManager.class);

    if (customDelegationTokenMgrClass == null) {
      throw new IllegalArgumentException(
              "The value for \"fs.azure.delegation.token.provider.type\" is not defined.");
    }

    CustomDelegationTokenManager customTokenMgr = (CustomDelegationTokenManager) ReflectionUtils
            .newInstance(customDelegationTokenMgrClass, conf);
    if (customTokenMgr == null) {
      throw new IllegalArgumentException(String.format("Failed to initialize %s.", customDelegationTokenMgrClass));
    }

    customTokenMgr.initialize(conf);

    tokenManager = customTokenMgr;
  }

  public Token<DelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException {

    Token<DelegationTokenIdentifier> token = tokenManager.getDelegationToken(renewer);

    token.setKind(AbfsDelegationTokenIdentifier.TOKEN_KIND);
    return token;
  }

  public long renewDelegationToken(Token<?> token)
      throws IOException {

    return tokenManager.renewDelegationToken(token);
  }

  public void cancelDelegationToken(Token<?> token)
          throws IOException {

    tokenManager.cancelDelegationToken(token);
  }
}
