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

import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;

/**
 * Helper utility for sending portmap response.
 */
public class PortmapResponse {
  public static XDR voidReply(XDR xdr, int xid) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    return xdr;
  }

  public static XDR intReply(XDR xdr, int xid, int value) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    xdr.writeInt(value);
    return xdr;
  }

  public static XDR booleanReply(XDR xdr, int xid, boolean value) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    xdr.writeBoolean(value);
    return xdr;
  }

  public static XDR pmapList(XDR xdr, int xid, PortmapMapping[] list) {
    RpcAcceptedReply.getAcceptInstance(xid, new VerifierNone()).write(xdr);
    for (PortmapMapping mapping : list) {
      xdr.writeBoolean(true); // Value follows
      mapping.serialize(xdr);
    }
    xdr.writeBoolean(false); // No value follows
    return xdr;
  }
}
