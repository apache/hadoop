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
package org.apache.hadoop.security.token;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for issuing delegation tokens.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "Yarn"})
@InterfaceStability.Unstable
public interface DelegationTokenIssuer {

  /**
   * The service name used as the alias for the  token in the credential
   * token map.  addDelegationTokens will use this to determine if
   * a token exists, and if not, add a new token with this alias.
   */
  String getCanonicalServiceName();

  /**
   * Unconditionally get a new token with the optional renewer.  Returning
   * null indicates the service does not issue tokens.
   */
  Token<?> getDelegationToken(String renewer) throws IOException;

  /**
   * Issuers may need tokens from additional services.
   */
  default DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    return null;
  }

  /**
   * Given a renewer, add delegation tokens for issuer and it's child issuers
   * to the <code>Credentials</code> object if it is not already present.
   *<p>
   * Note: This method is not intended to be overridden.  Issuers should
   * implement getCanonicalService and getDelegationToken to ensure
   * consistent token acquisition behavior.
   *
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add new delegation tokens
   * @return list of new delegation tokens
   * @throws IOException thrown if IOException if an IO error occurs.
   */
  default Token<?>[] addDelegationTokens(
      final String renewer, Credentials credentials) throws IOException {
    if (credentials == null) {
      credentials = new Credentials();
    }
    final List<Token<?>> tokens = new ArrayList<>();
    collectDelegationTokens(this, renewer, credentials, tokens);
    return tokens.toArray(new Token<?>[tokens.size()]);
  }

  /**
   * NEVER call this method directly.
   */
  @InterfaceAudience.Private
  static void collectDelegationTokens(
      final DelegationTokenIssuer issuer,
      final String renewer,
      final Credentials credentials,
      final List<Token<?>> tokens) throws IOException {
    final String serviceName = issuer.getCanonicalServiceName();
    // Collect token of the this issuer and then of its embedded children
    if (serviceName != null) {
      final Text service = new Text(serviceName);
      Token<?> token = credentials.getToken(service);
      if (token == null) {
        token = issuer.getDelegationToken(renewer);
        if (token != null) {
          tokens.add(token);
          credentials.addToken(service, token);
        }
      }
    }
    // Now collect the tokens from the children.
    final DelegationTokenIssuer[] ancillary =
        issuer.getAdditionalTokenIssuers();
    if (ancillary != null) {
      for (DelegationTokenIssuer subIssuer : ancillary) {
        collectDelegationTokens(subIssuer, renewer, credentials, tokens);
      }
    }
  }
}