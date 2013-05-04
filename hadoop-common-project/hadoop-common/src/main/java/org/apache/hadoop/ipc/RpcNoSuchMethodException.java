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
package org.apache.hadoop.ipc;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;


/**
 * No such Method for an Rpc Call
 *
 */
public class RpcNoSuchMethodException extends RpcServerException {
  private static final long serialVersionUID = 1L;
  public RpcNoSuchMethodException(final String message) {
    super(message);
  }
  
  /**
   * get the rpc status corresponding to this exception
   */
  public RpcStatusProto getRpcStatusProto() {
    return RpcStatusProto.ERROR;
  }

  /**
   * get the detailed rpc status corresponding to this exception
   */
  public RpcErrorCodeProto getRpcErrorCodeProto() {
    return RpcErrorCodeProto.ERROR_NO_SUCH_METHOD;
  }
}
