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

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.async.Async;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public final class AsyncRpcProtocolPBUtil {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncRpcProtocolPBUtil.class);

  private AsyncRpcProtocolPBUtil() {}

  public static  <T> AsyncGet<T, Exception> asyncIpc(
      ShadedProtobufHelper.IpcCall<T> call) throws IOException {
    CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    Client.CALL_FUTURE_THREAD_LOCAL.set(completableFuture);
    ipc(call);
    return (AsyncGet<T, Exception>) ProtobufRpcEngine2.getAsyncReturnMessage();
  }

  public static <T> void asyncResponse(Response<T> response) {
    CompletableFuture<T> callCompletableFuture =
        (CompletableFuture<T>) Client.CALL_FUTURE_THREAD_LOCAL.get();
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();
    CompletableFuture<Object> result = callCompletableFuture.thenApplyAsync(t -> {
      try {
        Server.getCurCall().set(originCall);
        CallerContext.setCurrent(originContext);
        return response.response();
      }catch (Exception e) {
        throw new CompletionException(e);
      }
    });
    Async.CUR_COMPLETABLE_FUTURE.set(result);
  }

  @FunctionalInterface
  interface Response<T> {
    T response() throws Exception;
  }
}