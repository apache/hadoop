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
package org.apache.hadoop.oncrpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Represents an RPC message of type RPC call as defined in RFC 1831
 */
public class RpcCall extends RpcMessage {
  public static final int RPC_VERSION = 2;
  private static final Log LOG = LogFactory.getLog(RpcCall.class);
  private final int rpcVersion;
  private final int program;
  private final int version;
  private final int procedure;
  private final RpcAuthInfo credential;
  private final RpcAuthInfo verifier;

  protected RpcCall(int xid, int messageType, int rpcVersion, int program,
      int version, int procedure, RpcAuthInfo credential, RpcAuthInfo verifier) {
    super(xid, messageType);
    this.rpcVersion = rpcVersion;
    this.program = program;
    this.version = version;
    this.procedure = procedure;
    this.credential = credential;
    this.verifier = verifier;
    if (LOG.isTraceEnabled()) {
      LOG.trace(this);
    }
    validate();
  }
  
  private void validateRpcVersion() {
    if (rpcVersion != RPC_VERSION) {
      throw new IllegalArgumentException("RPC version is expected to be "
          + RPC_VERSION + " but got " + rpcVersion);
    }
  }
  
  public void validate() {
    validateMessageType(RPC_CALL);
    validateRpcVersion();
    // Validate other members
    // Throw exception if validation fails
  }


  public int getRpcVersion() {
    return rpcVersion;
  }

  public int getProgram() {
    return program;
  }

  public int getVersion() {
    return version;
  }

  public int getProcedure() {
    return procedure;
  }
  
  public RpcAuthInfo getCredential() {
    return credential;
  }

  public RpcAuthInfo getVerifier() {
    return verifier;
  }
  
  public static RpcCall read(XDR xdr) {
    return new RpcCall(xdr.readInt(), xdr.readInt(), xdr.readInt(), xdr.readInt(),
        xdr.readInt(), xdr.readInt(), RpcAuthInfo.read(xdr),
        RpcAuthInfo.read(xdr));
  }
  
  public static void write(XDR out, int xid, int program, int progVersion,
      int procedure) {
    out.writeInt(xid);
    out.writeInt(RpcMessage.RPC_CALL);
    out.writeInt(2);
    out.writeInt(program);
    out.writeInt(progVersion);
    out.writeInt(procedure);
  }
  
  @Override
  public String toString() {
    return String.format("Xid:%d, messageType:%d, rpcVersion:%d, program:%d,"
        + " version:%d, procedure:%d, credential:%s, verifier:%s", xid,
        messageType, rpcVersion, program, version, procedure,
        credential.toString(), verifier.toString());
  }
}
