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


package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Interface for Managing the Delegation tokens.
 */
public interface CustomDelegationTokenManager {

  /**
   * Initialize with supported configuration. This method is invoked when the
   * (URI, Configuration)} method is invoked.
   *
   * @param configuration Configuration object
   * @throws IOException if instance can not be configured.
   */
  void initialize(Configuration configuration)
          throws IOException;

  /**
   * Get Delegation token.
   * @param renewer delegation token renewer
   * @return delegation token
   * @throws IOException when error in getting the delegation token
   */
  Token<DelegationTokenIdentifier> getDelegationToken(String renewer)
          throws IOException;

  /**
   * Renew the delegation token.
   * @param token delegation token.
   * @return renewed time.
   * @throws IOException when error in renewing the delegation token
   */
  long renewDelegationToken(Token<?> token) throws IOException;

  /**
   * Cancel the delegation token.
   * @param token delegation token.
   * @throws IOException when error in cancelling the delegation token.
   */
  void cancelDelegationToken(Token<?> token) throws IOException;
}
