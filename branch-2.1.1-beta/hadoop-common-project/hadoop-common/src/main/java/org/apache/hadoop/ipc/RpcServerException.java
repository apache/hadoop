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
 * Indicates an exception on the RPC server 
 */
public class RpcServerException extends RpcException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs exception with the specified detail message.
   * @param message detailed message.
   */
  public RpcServerException(final String message) {
    super(message);
  }
  
  /**
   * Constructs exception with the specified detail message and cause.
   * 
   * @param message message.
   * @param cause the cause (can be retried by the {@link #getCause()} method).
   *          (A <tt>null</tt> value is permitted, and indicates that the cause
   *          is nonexistent or unknown.)
   */
  public RpcServerException(final String message, final Throwable cause) {
    super(message, cause);
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
    return RpcErrorCodeProto.ERROR_RPC_SERVER;
  }
}
