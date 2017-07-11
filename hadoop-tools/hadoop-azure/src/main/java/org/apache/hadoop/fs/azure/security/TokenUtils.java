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

package org.apache.hadoop.fs.azure.security;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Utility methods common for token management
 */
public final class TokenUtils {
  public static final Logger LOG = LoggerFactory.getLogger(TokenUtils.class);
  public static final String URL_STRING = "urlString";

  private TokenUtils() {
  }

  public static Token<DelegationTokenIdentifier> toDelegationToken(
      final Map<?, ?> inputMap) throws IOException {
    final Map<?, ?> m = (Map<?, ?>) inputMap.get(Token.class.getSimpleName());
    return (Token<DelegationTokenIdentifier>) toToken(m);
  }

  public static Token<? extends TokenIdentifier> toToken(final Map<?, ?> m)
      throws IOException {
    if (m == null) {
      return null;
    }
    String urlString = (String) m.get(URL_STRING);
    if (urlString != null) {
      final Token<DelegationTokenIdentifier> token = new Token<>();
      LOG.debug("Read url string param - {}", urlString);
      token.decodeFromUrlString(urlString);
      return token;
    }
    return null;
  }
}