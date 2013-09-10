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
package org.apache.hadoop.oncrpc.security;

import org.apache.hadoop.oncrpc.XDR;

import com.google.common.base.Preconditions;

/** Credential used by AUTH_NONE */
public class CredentialsNone extends Credentials {

  public CredentialsNone() {
    super(AuthFlavor.AUTH_NONE);
    mCredentialsLength = 0;
  }

  @Override
  public void read(XDR xdr) {
    mCredentialsLength = xdr.readInt();
    Preconditions.checkState(mCredentialsLength == 0);
  }

  @Override
  public void write(XDR xdr) {
    Preconditions.checkState(mCredentialsLength == 0);
    xdr.writeInt(mCredentialsLength);
  }
}
