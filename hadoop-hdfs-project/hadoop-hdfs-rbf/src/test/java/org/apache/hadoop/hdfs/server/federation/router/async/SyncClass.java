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
package org.apache.hadoop.hdfs.server.federation.router.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * SyncClass implements BaseClass, providing a synchronous
 * version of the methods. All operations are performed in a
 * blocking manner, waiting for completion before proceeding.
 *
 * This class is the foundation for the AsyncClass, which
 * provides asynchronous implementations.
 *
 * @see BaseClass
 * @see AsyncClass
 */
public class SyncClass implements BaseClass{
  private long timeConsuming;

  public SyncClass(long timeConsuming) {
    this.timeConsuming = timeConsuming;
  }

  @Override
  public String applyMethod(int input) {
    String res = timeConsumingMethod(input);
    return "applyMethod" + res;
  }

  @Override
  public String applyMethod(int input, boolean canException) throws IOException {
    String res = timeConsumingMethod(input);
    if (canException) {
      if (res.equals("[2]")) {
        throw new IOException("input 2 exception");
      } else if (res.equals("[3]")) {
        throw new RuntimeException("input 3 exception");
      }
    }
    return res;
  }

  @Override
  public String exceptionMethod(int input) throws IOException {
    if (input == 2) {
      throw new IOException("input 2 exception");
    } else if (input == 3) {
      throw new RuntimeException("input 3 exception");
    }
    return applyMethod(input);
  }

  @Override
  public String forEachMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    for (int input : list) {
      String res = timeConsumingMethod(input);
      result.append("forEach" + res + ",");
    }
    return result.toString();
  }

  @Override
  public String forEachBreakMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    for (int input : list) {
      String res = timeConsumingMethod(input);
      if (res.equals("[2]")) {
        break;
      }
      result.append("forEach" + res + ",");
    }
    return result.toString();
  }

  @Override
  public String forEachBreakByExceptionMethod(List<Integer> list) {
    StringBuilder result = new StringBuilder();
    for (int input : list) {
      try {
        String res = applyMethod(input, true);
        result.append("forEach" + res + ",");
      } catch (IOException e) {
        result.append(e + ",");
      } catch (RuntimeException e) {
        break;
      }
    }
    return result.toString();
  }

  @Override
  public String applyThenApplyMethod(int input) {
    String res = timeConsumingMethod(input);
    if (res.equals("[1]")) {
      res = timeConsumingMethod(2);
    }
    return res;
  }

  @Override
  public String applyCatchThenApplyMethod(int input) {
    String res = null;
    try {
      res = applyMethod(input, true);
    } catch (IOException e) {
      res = applyMethod(1);
    }
    return res;
  }

  @Override
  public String applyCatchFinallyMethod(
      int input, List<String> resource) throws IOException {
    String res = null;
    try {
      res = applyMethod(input, true);
    } catch (IOException e) {
      throw new IOException("Catch " + e.getMessage());
    } finally {
      resource.clear();
    }
    return res;
  }

  @Override
  public String currentMethod(List<Integer> list) {
    ExecutorService executor = getExecutorService();
    List<Future<String>> futures = new ArrayList<>();
    for (int input : list) {
      Future<String> future = executor.submit(
          () -> applyMethod(input, true));
      futures.add(future);
    }

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
  }


  /**
   * Simulates a synchronous method that performs
   * a time-consuming task and returns a result.
   *
   * @param input The input parameter for the method.
   * @return A string that represents the result of the method.
   */
  public String timeConsumingMethod(int input) {
    try {
      Thread.sleep(timeConsuming);
      return "[" + input + "]";
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "Error:" + e.getMessage();
    }
  }

  private ExecutorService getExecutorService() {
    return Executors.newFixedThreadPool(2, r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    });
  }
}
