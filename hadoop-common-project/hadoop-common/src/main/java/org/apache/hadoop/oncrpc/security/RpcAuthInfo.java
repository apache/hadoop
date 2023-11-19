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

/**
 *  Authentication Info. Base class of Verifier and Credential.
 */
public abstract class RpcAuthInfo {
  /** Different types of authentication as defined in RFC 1831 */
  public enum AuthFlavor {
    AUTH_NONE(0),
    AUTH_SYS(1),
    AUTH_SHORT(2),
    AUTH_DH(3),
    RPCSEC_GSS(6);
    
    private int value;
    
    AuthFlavor(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return value;
    }
    
    static AuthFlavor fromValue(int value) {
      for (AuthFlavor v : values()) {
        if (v.value == value) {
          return v;
        }
      }
      throw new IllegalArgumentException("Invalid AuthFlavor value " + value);
    }
  }
  
  private final AuthFlavor flavor;
  
  protected RpcAuthInfo(AuthFlavor flavor) {
    this.flavor = flavor;
  }
  
  /**
   * Load auth info.
   * @param xdr XDR message
   */
  public abstract void read(XDR xdr);
  
  /** Write auth info.
   * @param xdr XDR message
   */
  public abstract void write(XDR xdr);
  
  public AuthFlavor getFlavor() {
    return flavor;
  }
  
  @Override
  public String toString() {
    return "(AuthFlavor:" + flavor + ")";
  }
}
