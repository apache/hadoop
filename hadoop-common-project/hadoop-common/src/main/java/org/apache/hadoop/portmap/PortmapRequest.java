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
package org.apache.hadoop.portmap;

import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;

/**
 * Helper utility for building portmap request
 */
public class PortmapRequest {
  public static PortmapMapping mapping(XDR xdr) {
    return PortmapMapping.deserialize(xdr);
  }

  public static XDR create(PortmapMapping mapping, boolean set) {
    XDR request = new XDR();
    int procedure = set ? RpcProgramPortmap.PMAPPROC_SET
        : RpcProgramPortmap.PMAPPROC_UNSET;
    RpcCall call = RpcCall.getInstance(
        RpcUtil.getNewXid(String.valueOf(RpcProgramPortmap.PROGRAM)),
        RpcProgramPortmap.PROGRAM, RpcProgramPortmap.VERSION, procedure,
        new CredentialsNone(), new VerifierNone());
    call.write(request);
    return mapping.serialize(request);
  }
}
