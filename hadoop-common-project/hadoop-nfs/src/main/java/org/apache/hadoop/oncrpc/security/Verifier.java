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
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;

/**
 * Base class for verifier. Currently our authentication only supports 3 types
 * of auth flavors: {@link AuthFlavor#AUTH_NONE}, {@link AuthFlavor#AUTH_SYS},
 * and {@link AuthFlavor#RPCSEC_GSS}. Thus for verifier we only need to handle
 * AUTH_NONE and RPCSEC_GSS
 */
public abstract class Verifier extends RpcAuthInfo {

  protected Verifier(AuthFlavor flavor) {
    super(flavor);
  }

  /** Read both AuthFlavor and the verifier from the XDR */
  public static Verifier readFlavorAndVerifier(XDR xdr) {
    AuthFlavor flavor = AuthFlavor.fromValue(xdr.readInt());
    final Verifier verifer;
    if(flavor == AuthFlavor.AUTH_NONE) {
      verifer = new VerifierNone();
    } else if(flavor == AuthFlavor.RPCSEC_GSS) {
      verifer = new VerifierGSS();
    } else {
      throw new UnsupportedOperationException("Unsupported verifier flavor"
          + flavor);
    }
    verifer.read(xdr);
    return verifer;
  }
  
  /**
   * Write AuthFlavor and the verifier to the XDR
   */
  public static void writeFlavorAndVerifier(Verifier verifier, XDR xdr) {
    if (verifier instanceof VerifierNone) {
      xdr.writeInt(AuthFlavor.AUTH_NONE.getValue());
    } else if (verifier instanceof VerifierGSS) {
      xdr.writeInt(AuthFlavor.RPCSEC_GSS.getValue());
    } else {
      throw new UnsupportedOperationException("Cannot recognize the verifier");
    }
    verifier.write(xdr);
  }  
 
  
}
