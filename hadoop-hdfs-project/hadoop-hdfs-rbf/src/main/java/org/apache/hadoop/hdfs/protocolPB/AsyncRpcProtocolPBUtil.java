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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
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
  private static Executor worker;

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
    }, worker));
    return asyncReturn(clazz);
  }

  /**
   * Sets the executor used for handling responses asynchronously within
   * the utility class.
   *
   * @param worker The executor to be used for handling responses asynchronously.
   */
  public static void setWorker(Executor worker) {
    AsyncRpcProtocolPBUtil.worker = worker;
  }
}
