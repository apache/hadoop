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

import org.apache.hadoop.hdfs.server.federation.router.ThreadLocalContext;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtobufRpcEngineCallback2;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCompleteWith;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * <p>This utility class encapsulates the logic required to initiate asynchronous RPCs,
 * handle responses, and propagate exceptions. It works in conjunction with
 * {@link ProtobufRpcEngine2} and {@link Client} to facilitate the asynchronous
 * nature of the operations.
 *
 * @see ProtobufRpcEngine2
 * @see Client
 * @see CompletableFuture
 */
public final class AsyncRpcProtocolPBUtil {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncRpcProtocolPBUtil.class);
  /** The executor used for handling responses asynchronously. */
  private static Executor asyncResponderExecutor;

  private AsyncRpcProtocolPBUtil() {}

  /**
   * Asynchronously invokes an RPC call and applies a response transformation function
   * to the result. This method is generic and can be used to handle any type of
   * RPC call.
   *
   * <p>The method uses the {@link ShadedProtobufHelper.IpcCall} to prepare the RPC call
   * and the {@link ApplyFunction} to process the response. It also handles exceptions
   * that may occur during the RPC call and wraps them in a user-friendly manner.
   *
   * @param call The IPC call encapsulating the RPC request.
   * @param response The function to apply to the response of the RPC call.
   * @param clazz The class object representing the type {@code R} of the response.
   * @param <T> Type of the call's result.
   * @param <R> Type of method return.
   * @return An object of type {@code R} that is the result of applying the response
   *         function to the RPC call result.
   * @throws IOException If an I/O error occurs during the asynchronous RPC call.
   */
  public static <T, R> R asyncIpcClient(
      ShadedProtobufHelper.IpcCall<T> call, ApplyFunction<T, R> response,
      Class<R> clazz) throws IOException {
    ipc(call);
    AsyncGet<T, Exception> asyncReqMessage =
        (AsyncGet<T, Exception>) ProtobufRpcEngine2.getAsyncReturnMessage();
    CompletableFuture<Writable> responseFuture = Client.getResponseFuture();
    // transfer thread local context to worker threads of executor.
    ThreadLocalContext threadLocalContext = new ThreadLocalContext();
    asyncCompleteWith(responseFuture.handleAsync((result, e) -> {
      threadLocalContext.transfer();
      if (e != null) {
        throw warpCompletionException(e);
      }
      try {
        T res = asyncReqMessage.get(-1, null);
        return response.apply(res);
      } catch (Exception ex) {
        throw warpCompletionException(ex);
      }
    }, asyncResponderExecutor));
    return asyncReturn(clazz);
  }

  /**
   * Asynchronously invokes an RPC call and applies a response transformation function
   * to the result on server-side.
   * @param req The IPC call encapsulating the RPC request on server-side.
   * @param res The function to apply to the response of the RPC call on server-side.
   * @param <T> Type of the call's result.
   */
  public static <T> void asyncRouterServer(ServerReq<T> req, ServerRes<T> res) {
    final ProtobufRpcEngineCallback2 callback =
        ProtobufRpcEngine2.Server.registerForDeferredResponse2();

    CompletableFuture<Object> completableFuture =
        CompletableFuture.completedFuture(null);
    completableFuture.thenCompose(o -> {
      try {
        req.req();
        return (CompletableFuture<T>) AsyncUtil.getAsyncUtilCompletableFuture();
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }).handle((result, e) -> {
      LOG.debug("Async response, callback: {}, CallerContext: {}, result: [{}], exception: [{}]",
          callback, CallerContext.getCurrent(), result, e);
      if (e == null) {
        Message value = null;
        try {
          value = res.res(result);
        } catch (Exception re) {
          callback.error(re);
          return null;
        }
        callback.setResponse(value);
      } else {
        callback.error(e.getCause());
      }
      return null;
    });
  }

  /**
   * Sets the executor used for handling responses asynchronously within
   * the utility class.
   *
   * @param asyncResponderExecutor The executor to be used for handling responses asynchronously.
   */
  public static void setAsyncResponderExecutor(Executor asyncResponderExecutor) {
    AsyncRpcProtocolPBUtil.asyncResponderExecutor = asyncResponderExecutor;
  }

  @FunctionalInterface
  interface ServerReq<T> {
    T req() throws Exception;
  }

  @FunctionalInterface
  interface ServerRes<T> {
    Message res(T result) throws Exception;
  }
}
