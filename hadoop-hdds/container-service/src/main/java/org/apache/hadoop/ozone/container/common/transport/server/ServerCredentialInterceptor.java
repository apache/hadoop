/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.transport.server;

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.io.grpc.Context;
import org.apache.ratis.thirdparty.io.grpc.Contexts;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;

import static org.apache.hadoop.ozone.OzoneConsts.OBT_METADATA_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.USER_METADATA_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.UGI_CTX_KEY;
/**
 * Grpc Server Interceptor for Ozone Block token.
 */
public class ServerCredentialInterceptor implements ServerInterceptor {


  private static final ServerCall.Listener NOOP_LISTENER =
      new ServerCall.Listener() {
  };

  private final TokenVerifier verifier;

  ServerCredentialInterceptor(TokenVerifier verifier) {
    this.verifier = verifier;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    String token = headers.get(OBT_METADATA_KEY);
    String user = headers.get(USER_METADATA_KEY);
    Context ctx = Context.current();
    try {
      UserGroupInformation ugi = verifier.verify(user, token);
      if (ugi == null) {
        call.close(Status.UNAUTHENTICATED.withDescription("Missing Block " +
            "Token from headers when block token is required."), headers);
        return NOOP_LISTENER;
      } else {
        ctx = ctx.withValue(UGI_CTX_KEY, ugi);
      }
    } catch (SCMSecurityException e) {
      call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage())
          .withCause(e), headers);
      return NOOP_LISTENER;
    }
    return Contexts.interceptCall(ctx, call, headers, next);
  }
}