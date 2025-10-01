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
package org.apache.hadoop.hdfs.server.federation.router.async.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCatch;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCurrent;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncFinally;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncForEach;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncThrowException;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncTry;

/**
 * AsyncClass demonstrates the conversion of synchronous methods
 * from SyncClass into asynchronous operations using AsyncUtil.
 * This class overrides methods with asynchronous logic, enhancing
 * the performance by allowing non-blocking task execution.
 *
 * <p>
 * By utilizing AsyncUtil's utility methods, such as asyncApply,
 * asyncForEach, and others, each method in AsyncClass can perform
 * time-consuming tasks on a separate thread, thus not blocking
 * the main execution thread.
 * </p>
 *
 * <p>
 * For example, the applyMethod in AsyncClass is an async version of
 * the same method in SyncClass. It uses asyncApply to schedule
 * the timeConsumingMethod to run asynchronously and returns a
 * CompletableFuture that will be completed with the result of
 * the operation.
 * </p>
 *
 * <p>
 * This class serves as an example of how to transform synchronous
 * operations into asynchronous ones using the AsyncUtil tools,
 * which can be applied to other parts of the HDFS Federation
 * router or similar systems to improve concurrency and
 * performance.
 * </p>
 *
 * @see SyncClass
 * @see AsyncUtil
 * @see CompletableFuture
 */
public class AsyncClass extends SyncClass{
  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncClass.class);
  private ExecutorService executorService;
  private final static String ASYNC_WORKER = "Async Worker";

  public AsyncClass(long timeConsuming) {
    super(timeConsuming);
    executorService = Executors.newFixedThreadPool(1, r -> {
      Thread asyncWork = new Thread(r);
      asyncWork.setDaemon(true);
      asyncWork.setName(ASYNC_WORKER);
      return asyncWork;
    });
  }

  @Override
  public String applyMethod(int input) {
    timeConsumingMethod(input);
    asyncApply(res -> {
      return "applyMethod" + res;
    });
    return asyncReturn(String.class);
  }

  @Override
  public String applyMethod(int input, boolean canException) {
    timeConsumingMethod(input);
    asyncApply(res -> {
      if (canException) {
        if (res.equals("[2]")) {
          throw new IOException("input 2 exception");
        } else if (res.equals("[3]")) {
          throw new RuntimeException("input 3 exception");
        }
      }
      return res;
    });
    return asyncReturn(String.class);
  }

  @Override
  public String exceptionMethod(int input) {
    if (input == 2) {
      asyncThrowException(new IOException("input 2 exception"));
      return null;
    } else if (input == 3) {
      asyncThrowException(new RuntimeException("input 3 exception"));
      return null;
    }
    return applyMethod(input);
  }

  @Override
  public String forEachMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    asyncForEach(list.iterator(),
        (forEach, input) -> {
          timeConsumingMethod(input);
          asyncApply(res -> {
            result.append("forEach" + res + ",");
            return result.toString();
          });
        });
    return asyncReturn(String.class);
  }

  @Override
  public String forEachBreakMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    asyncForEach(list.iterator(),
        (forEach, input) -> {
          timeConsumingMethod(input);
          asyncApply(res -> {
            if (res.equals("[2]")) {
              forEach.breakNow();
            } else {
              result.append("forEach" + res + ",");
            }
            return result.toString();
          });
        });
    return asyncReturn(String.class);
  }

  @Override
  public String forEachBreakByExceptionMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    asyncForEach(list.iterator(),
        (forEach, input) -> {
          asyncTry(() -> {
            applyMethod(input, true);
            asyncApply(res -> {
              result.append("forEach" + res + ",");
              return result.toString();
            });
          });
          asyncCatch((res, e) -> {
            if (e instanceof IOException) {
              result.append(e + ",");
            } else if (e instanceof RuntimeException) {
              forEach.breakNow();
            }
            return result.toString();
          }, Exception.class);
        });
    return asyncReturn(String.class);
  }

  @Override
  public String applyThenApplyMethod(int input) {
    timeConsumingMethod(input);
    asyncApply((AsyncApplyFunction<String, String>) res -> {
      if (res.equals("[1]")) {
        timeConsumingMethod(2);
      } else {
        asyncComplete(res);
      }
    });
    return asyncReturn(String.class);
  }

  @Override
  public String applyCatchThenApplyMethod(int input) {
    asyncTry(() -> applyMethod(input, true));
    asyncCatch((AsyncCatchFunction<String, IOException>) (res, ioe) -> {
      applyMethod(1);
    }, IOException.class);
    return asyncReturn(String.class);
  }

  @Override
  public String applyCatchFinallyMethod(
      int input, List<String> resource) {
    asyncTry(() -> applyMethod(input, true));
    asyncCatch((res, e) -> {
      throw new IOException("Catch " + e.getMessage());
    }, IOException.class);
    asyncFinally((FinallyFunction<String>) res -> {
      resource.clear();
      return res;
    });
    return asyncReturn(String.class);
  }

  @Override
  public String currentMethod(List<Integer> list) {
    asyncCurrent(list,
        input -> applyMethod(input, true),
        (Function<CompletableFuture<String>[], String>) futures -> {
          StringBuilder result = new StringBuilder();
          for (Future<String> future : futures) {
            try {
              String res = future.get();
              result.append(res + ",");
            } catch (Exception e) {
              result.append(e.getMessage() + ",");
            }
          }
          return result.toString();
        });
    return asyncReturn(String.class);
  }

  @Override
  public String timeConsumingMethod(int input) {
    CompletableFuture<Object> result = CompletableFuture
        .supplyAsync(() -> {
          LOG.info("[{} thread] invoke consumingMethod for parameter: {}",
              Thread.currentThread().getName(), input);
          return AsyncClass.super.timeConsumingMethod(input);
        }, executorService);
    Async.CUR_COMPLETABLE_FUTURE.set(result);
    return null;
  }
}
