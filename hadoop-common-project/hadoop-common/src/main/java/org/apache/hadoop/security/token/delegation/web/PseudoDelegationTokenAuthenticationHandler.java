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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;

/**
 * An {@link AuthenticationHandler} that implements Kerberos SPNEGO mechanism
 * for HTTP and supports Delegation Token functionality.
 * <p>
 * In addition to the {@link KerberosAuthenticationHandler} configuration
 * properties, this handler supports:
 * <ul>
 * <li>simple.delegation-token.token-kind: the token kind for generated tokens
 * (no default, required property).</li>
 * <li>simple.delegation-token.update-interval.sec: secret manager master key
 * update interval in seconds (default 1 day).</li>
 * <li>simple.delegation-token.max-lifetime.sec: maximum life of a delegation
 * token in seconds (default 7 days).</li>
 * <li>simple.delegation-token.renewal-interval.sec: renewal interval for
 * delegation tokens in seconds (default 1 day).</li>
 * <li>simple.delegation-token.removal-scan-interval.sec: delegation tokens
 * removal scan interval in seconds (default 1 hour).</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PseudoDelegationTokenAuthenticationHandler
    extends DelegationTokenAuthenticationHandler {

  public PseudoDelegationTokenAuthenticationHandler() {
    super(new PseudoAuthenticationHandler(PseudoAuthenticationHandler.TYPE +
        TYPE_POSTFIX));
  }

}
