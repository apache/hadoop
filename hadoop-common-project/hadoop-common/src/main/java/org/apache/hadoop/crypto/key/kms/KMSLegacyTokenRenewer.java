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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.KMSUtil;

import java.io.IOException;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_LEGACY_KIND;

/**
 * The {@link KMSTokenRenewer} that supports legacy tokens.
 */
@InterfaceAudience.Private
@Deprecated
public class KMSLegacyTokenRenewer extends KMSTokenRenewer {

  @Override
  public boolean handleKind(Text kind) {
    return kind.equals(TOKEN_LEGACY_KIND);
  }

  /**
   * Create a key provider for token renewal / cancellation.
   * Caller is responsible for closing the key provider.
   */
  @Override
  protected KeyProvider createKeyProvider(Token<?> token,
      Configuration conf) throws IOException {
    assert token.getKind().equals(TOKEN_LEGACY_KIND);
    // Legacy tokens get service from configuration.
    return KMSUtil.createKeyProvider(conf,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
  }
}
