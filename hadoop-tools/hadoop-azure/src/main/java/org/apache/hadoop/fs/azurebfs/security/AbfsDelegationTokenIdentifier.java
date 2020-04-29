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


package org.apache.hadoop.fs.azurebfs.security;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/**
 * Delegation token Identifier for ABFS delegation tokens.
 * The token kind from {@link #getKind()} is {@link #TOKEN_KIND}, always.
 *
 * Subclasses have to very careful when looking up tokens (which will of
 * course be registered in the credentials as of this kind), in case the
 * incoming credentials are actually of a different subtype.
 */
public class AbfsDelegationTokenIdentifier extends DelegationTokenIdentifier {

  /**
   * The token kind of these tokens: ""ABFS delegation".
   */
  public static final Text TOKEN_KIND = new Text("ABFS delegation");

  public AbfsDelegationTokenIdentifier(){
    super(TOKEN_KIND);
  }

  public AbfsDelegationTokenIdentifier(Text kind) {
    super(kind);
  }

  public AbfsDelegationTokenIdentifier(Text kind, Text owner, Text renewer,
      Text realUser) {
    super(kind, owner, renewer, realUser);
  }

  /**
   * Get the token kind.
   * Returns {@link #TOKEN_KIND} always.
   * If a subclass does not want its renew/cancel process to be managed
   * by {@link AbfsDelegationTokenManager}, this must be overridden.
   * @return the kind of the token.
   */
  @Override
  public Text getKind() {
    return TOKEN_KIND;
  }

}
