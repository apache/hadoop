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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

import java.io.IOException;

/**
 * The <code>PseudoDelegationTokenAuthenticator</code> provides support for
 * Hadoop's pseudo authentication mechanism that accepts
 * the user name specified as a query string parameter and support for Hadoop
 * Delegation Token operations.
 * <p/>
 * This mimics the model of Hadoop Simple authentication trusting the
 * {@link UserGroupInformation#getCurrentUser()} value.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PseudoDelegationTokenAuthenticator
    extends DelegationTokenAuthenticator {

  public PseudoDelegationTokenAuthenticator() {
    super(new PseudoAuthenticator() {
      @Override
      protected String getUserName() {
        try {
          return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    });
  }

}
