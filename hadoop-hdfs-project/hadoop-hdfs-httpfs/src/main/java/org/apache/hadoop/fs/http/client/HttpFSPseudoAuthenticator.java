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

package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

import java.io.IOException;

/**
 * A <code>PseudoAuthenticator</code> subclass that uses FileSystemAccess's
 * <code>UserGroupInformation</code> to obtain the client user name (the UGI's login user).
 */
@InterfaceAudience.Private
public class HttpFSPseudoAuthenticator extends PseudoAuthenticator {

  /**
   * Return the client user name.
   *
   * @return the client user name.
   */
  @Override
  protected String getUserName() {
    try {
      return UserGroupInformation.getLoginUser().getUserName();
    } catch (IOException ex) {
      throw new SecurityException("Could not obtain current user, " + ex.getMessage(), ex);
    }
  }
}
