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
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;

/**
 * The <code>KerberosDelegationTokenAuthenticator</code> provides support for
 * Kerberos SPNEGO authentication mechanism and support for Hadoop Delegation
 * Token operations.
 * <p/>
 * It falls back to the {@link PseudoDelegationTokenAuthenticator} if the HTTP
 * endpoint does not trigger a SPNEGO authentication
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KerberosDelegationTokenAuthenticator
    extends DelegationTokenAuthenticator {

  public KerberosDelegationTokenAuthenticator() {
    super(new KerberosAuthenticator() {
      @Override
      protected Authenticator getFallBackAuthenticator() {
        return new PseudoDelegationTokenAuthenticator();
      }
    });
  }
}
