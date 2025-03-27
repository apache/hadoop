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

package org.apache.hadoop.yarn.server.nodemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class NMDelegationTokenManager {

  private final Configuration conf;

  public NMDelegationTokenManager(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Renews a token on behalf of the user logged in.
   * @param token Token to be renewed
   * @return Expiration time for the token
   * @throws IOException raised on errors performing I/O.
   * @throws InterruptedException if the thread is interrupted.
   */
  public Long renewToken(Token<? extends TokenIdentifier> token)
      throws IOException, InterruptedException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    return ugi.doAs((PrivilegedExceptionAction<Long>) () -> token.renew(conf));
  }
}
