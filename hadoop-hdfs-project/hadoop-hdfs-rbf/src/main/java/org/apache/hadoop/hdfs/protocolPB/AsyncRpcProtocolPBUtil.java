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

import org.apache.hadoop.hdfs.server.federation.router.async.ApplyFunction;
import org.apache.hadoop.io.Writable;
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

import static org.apache.hadoop.hdfs.server.federation.router.async.Async.warpCompletionException;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncCompleteWith;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public final class AsyncRpcProtocolPBUtil {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncRpcProtocolPBUtil.class);

  private AsyncRpcProtocolPBUtil() {}

  public static <T, R> R asyncIpcClient(
      ShadedProtobufHelper.IpcCall<T> call, ApplyFunction<T, R> response,
      Class<R> clazz) throws IOException {
    ipc(call);
    AsyncGet<T, Exception> asyncReqMessage =
        (AsyncGet<T, Exception>) ProtobufRpcEngine2.getAsyncReturnMessage();
    CompletableFuture<Writable> responseFuture = Client.getResponseFuture();
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();
    asyncCompleteWith(responseFuture);
    asyncApply(o -> {
      try {
        Server.getCurCall().set(originCall);
        CallerContext.setCurrent(originContext);
        T res = asyncReqMessage.get(-1, null);
        return response.apply(res);
      } catch (Exception e) {
        throw warpCompletionException(e);
      }
    });
    return asyncReturn(clazz);
  }
}
