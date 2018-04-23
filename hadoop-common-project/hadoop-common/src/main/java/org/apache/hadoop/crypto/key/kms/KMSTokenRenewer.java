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
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.KMSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_KIND;

/**
 * The KMS implementation of {@link TokenRenewer}.
 */
@InterfaceAudience.Private
public class KMSTokenRenewer extends TokenRenewer {

  public static final Logger LOG = LoggerFactory
      .getLogger(org.apache.hadoop.crypto.key.kms.KMSTokenRenewer.class);

  @Override
  public boolean handleKind(Text kind) {
    return kind.equals(TOKEN_KIND);
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException {
    LOG.debug("Renewing delegation token {}", token);
    final KeyProvider keyProvider = createKeyProvider(token, conf);
    try {
      if (!(keyProvider instanceof
          KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
        throw new IOException(String
            .format("keyProvider %s cannot renew token [%s]",
                keyProvider == null ? "null" : keyProvider.getClass(), token));
      }
      return ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
          keyProvider).renewDelegationToken(token);
    } finally {
      if (keyProvider != null) {
        keyProvider.close();
      }
    }
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException {
    LOG.debug("Canceling delegation token {}", token);
    final KeyProvider keyProvider = createKeyProvider(token, conf);
    try {
      if (!(keyProvider instanceof
          KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
        throw new IOException(String
            .format("keyProvider %s cannot cancel token [%s]",
                keyProvider == null ? "null" : keyProvider.getClass(), token));
      }
      ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
          keyProvider).cancelDelegationToken(token);
    } finally {
      if (keyProvider != null) {
        keyProvider.close();
      }
    }
  }

  /**
   * Create a key provider for token renewal / cancellation.
   * Caller is responsible for closing the key provider.
   */
  protected KeyProvider createKeyProvider(Token<?> token,
      Configuration conf) throws IOException {
    return KMSUtil
        .createKeyProviderFromTokenService(conf, token.getService().toString());
  }
}
