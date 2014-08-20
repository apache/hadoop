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
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;

import javax.servlet.ServletException;
import java.util.Properties;

public class HttpFSKerberosAuthenticationHandlerForTesting
  extends KerberosDelegationTokenAuthenticationHandler {

  @Override
  public void init(Properties config) throws ServletException {
    //NOP overwrite to avoid Kerberos initialization
    config.setProperty(TOKEN_KIND, "t");
    initTokenManager(config);
  }

  @Override
  public void destroy() {
    //NOP overwrite to avoid Kerberos initialization
  }
}
