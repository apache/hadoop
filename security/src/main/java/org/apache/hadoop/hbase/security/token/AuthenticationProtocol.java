/*
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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.security.token.Token;

/**
 * Defines a custom RPC protocol for obtaining authentication tokens
 */
public interface AuthenticationProtocol extends CoprocessorProtocol {
  /**
   * Obtains a token capable of authenticating as the current user for future
   * connections.
   * @return an authentication token for the current user
   * @throws IOException If obtaining a token is denied or encounters an error
   */
  public Token<AuthenticationTokenIdentifier> getAuthenticationToken()
      throws IOException;

  /**
   * Returns the currently authenticated username.
   */
  public String whoami();
}
