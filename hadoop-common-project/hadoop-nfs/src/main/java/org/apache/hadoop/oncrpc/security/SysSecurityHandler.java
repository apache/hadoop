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

import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.security.IdMappingConstant;
import org.apache.hadoop.security.IdMappingServiceProvider;

public class SysSecurityHandler extends SecurityHandler {
  
  private final IdMappingServiceProvider iug;
  private final CredentialsSys mCredentialsSys;
  
  public SysSecurityHandler(CredentialsSys credentialsSys,
      IdMappingServiceProvider iug) {
    this.mCredentialsSys = credentialsSys;
    this.iug = iug;
  }
  
  @Override
  public String getUser() {
    return iug.getUserName(mCredentialsSys.getUID(),
        IdMappingConstant.UNKNOWN_USER);
  }

  @Override
  public boolean shouldSilentlyDrop(RpcCall request) {
    return false;
  }

  @Override
  public VerifierNone getVerifer(RpcCall request) {
    return new VerifierNone();
  }
  
  @Override
  public int getUid() {
    return mCredentialsSys.getUID();
  }
  
  @Override
  public int getGid() {
    return mCredentialsSys.getGID();
  }

  @Override
  public int[] getAuxGids() {
    return mCredentialsSys.getAuxGIDs();
  }
}
