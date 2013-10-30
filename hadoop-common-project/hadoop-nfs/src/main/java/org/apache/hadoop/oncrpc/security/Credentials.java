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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.XDR;

/**
 * Base class for all credentials. Currently we only support 3 different types
 * of auth flavors: AUTH_NONE, AUTH_SYS, and RPCSEC_GSS.
 */
public abstract class Credentials extends RpcAuthInfo {
  public static final Log LOG = LogFactory.getLog(Credentials.class);

  public static Credentials readFlavorAndCredentials(XDR xdr) {
    AuthFlavor flavor = AuthFlavor.fromValue(xdr.readInt());
    final Credentials credentials;
    if(flavor == AuthFlavor.AUTH_NONE) {
      credentials = new CredentialsNone();
    } else if(flavor == AuthFlavor.AUTH_SYS) {
      credentials = new CredentialsSys();
    } else if(flavor == AuthFlavor.RPCSEC_GSS) {
      credentials = new CredentialsGSS();
    } else {
      throw new UnsupportedOperationException("Unsupported Credentials Flavor "
          + flavor);
    }
    credentials.read(xdr);
    return credentials;
  }
  
  /**
   * Write AuthFlavor and the credentials to the XDR
   */
  public static void writeFlavorAndCredentials(Credentials cred, XDR xdr) {
    if (cred instanceof CredentialsNone) {
      xdr.writeInt(AuthFlavor.AUTH_NONE.getValue());
    } else if (cred instanceof CredentialsSys) {
      xdr.writeInt(AuthFlavor.AUTH_SYS.getValue());
    } else if (cred instanceof CredentialsGSS) {
      xdr.writeInt(AuthFlavor.RPCSEC_GSS.getValue());
    } else {
      throw new UnsupportedOperationException("Cannot recognize the verifier");
    }
    cred.write(xdr);
  }
  
  protected int mCredentialsLength;
  
  protected Credentials(AuthFlavor flavor) {
    super(flavor);
  }
}
