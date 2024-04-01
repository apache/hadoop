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

package org.apache.hadoop.fs.azurebfs.services.kac;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.util.functional.FutureIO;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testBasicPool() throws IOException {
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    validatePoolSize(DEFAULT_MAX_CONN_SYS_PROP);
  }

  @Test
  public void testSysPropAppliedPool() throws IOException {
    final String customPoolSize = "10";
    System.setProperty(HTTP_MAX_CONN_SYS_PROP, customPoolSize);
    validatePoolSize(Integer.parseInt(customPoolSize));
  }

  private void validatePoolSize(int size) throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    final HttpClientConnection[] connections = new HttpClientConnection[size
        * 2];

    for (int i = 0; i < size * 2; i++) {
      connections[i] = Mockito.mock(HttpClientConnection.class);
    }

    for (int i = 0; i < size * 2; i++) {
      keepAliveCache.put(routes, connections[i]);
    }

    for (int i = size; i < size * 2; i++) {
      Mockito.verify(connections[i], Mockito.times(1)).close();
    }

    for (int i = 0; i < size * 2; i++) {
      if (i < size) {
        Assert.assertNotNull(keepAliveCache.get(routes));
      } else {
        Assert.assertNull(keepAliveCache.get(routes));
      }
    }
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
  }

  @Test
  public void testKeepAliveCache() throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);

    keepAliveCache.put(routes, connection);

    Assert.assertNotNull(keepAliveCache.get(routes));
    keepAliveCache.put(routes, connection);

    final HttpRoute routes1 = new HttpRoute(new HttpHost("localhost1"));
    Assert.assertNull(keepAliveCache.get(routes1));
  }

  @Test
  public void testKeepAliveCacheCleanup() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * keepAliveCache.getConnectionIdleTTL());
    Mockito.verify(connection, Mockito.times(1)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.pauseThread();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * keepAliveCache.getConnectionIdleTTL());
    Mockito.verify(connection, Mockito.times(0)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
    keepAliveCache.resumeThread();
  }

  @Test
  public void testKeepAliveCacheParallelismAtSingleRoute() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    int parallelism = 4;
    ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
    /*
     * Verify the correctness of KeepAliveCache at single route level state
     * in a multi-threaded environment.
     */
    try {
      Stack<HttpClientConnection> stack = new Stack<>();
      HttpRoute routes = new HttpRoute(new HttpHost("host"));
      Future<?>[] futures = new Future[parallelism];
      for (int i = 0; i < parallelism; i++) {
        final boolean putRequest = i % 2 == 0;
        futures[i] = executorService.submit(() -> {
          for (int j = 0; j < DEFAULT_MAX_CONN_SYS_PROP * 4; j++) {
            synchronized (this) {
              if (putRequest) {
                HttpClientConnection connection = Mockito.mock(
                    HttpClientConnection.class);
                stack.add(connection);
                try {
                  Mockito.doAnswer(answer -> {
                    Assertions.assertThat(connection).isEqualTo(stack.pop());
                    return null;
                  }).when(connection).close();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                keepAliveCache.put(routes, connection);
              } else {
                try {
                  if (stack.empty()) {
                    Assertions.assertThat(keepAliveCache.get(routes)).isNull();
                  } else {
                    Assertions.assertThat(keepAliveCache.get(routes))
                        .isEqualTo(stack.pop());
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            }
          }
        });
      }
      for (int i = 0; i < parallelism; i++) {
        FutureIO.awaitFuture(futures[i]);
      }
      while (!stack.empty()) {
        Assertions.assertThat(keepAliveCache.get(routes))
            .isEqualTo(stack.pop());
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testKeepAliveCacheParallelismAtWithMultipleRoute()
      throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    try {
      /*
       * Verify that KeepAliveCache maintains connection at route level.
       */
      Future<?>[] futures = new Future[4];
      for (int i = 0; i < 4; i++) {
        /*
         * Create a route for each thread. Iteratively call put and get methods
         * on the KeepAliveCache for the same route. Verify that the connections
         * are being returned in the order they were put in the cache. Also, verify
         * that the cache is not holding more than the maximum number of connections.
         */
        final HttpRoute routes = new HttpRoute(new HttpHost("host" + i));
        futures[i] = executorService.submit(() -> {
          final Stack<HttpClientConnection> connections = new Stack<>();
          for (int j = 0; j < 3 * DEFAULT_MAX_CONN_SYS_PROP; j++) {
            if ((j / DEFAULT_MAX_CONN_SYS_PROP) % 2 == 0) {
              HttpClientConnection connection = Mockito.mock(
                  HttpClientConnection.class);
              connections.add(connection);
              try {
                Mockito.doAnswer(answer -> {
                  Assertions.assertThat(connection)
                      .isEqualTo(connections.pop());
                  return null;
                }).when(connection).close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              keepAliveCache.put(routes, connection);
            } else {
              try {
                Assertions.assertThat(keepAliveCache.get(routes))
                    .isEqualTo(connections.pop());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

          }
          Assertions.assertThat(connections).hasSize(DEFAULT_MAX_CONN_SYS_PROP);
        });
      }

      for (int i = 0; i < 4; i++) {
        FutureIO.awaitFuture(futures[i]);
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testKeepAliveCacheConnectionRecache() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Assert.assertNotNull(keepAliveCache.get(routes));
    keepAliveCache.put(routes, connection);
    Assert.assertNotNull(keepAliveCache.get(routes));
  }

  @Test
  public void testKeepAliveCacheRemoveStaleConnection() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.getInstance();
    keepAliveCache.clearThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection[] connections = new HttpClientConnection[5];
    for (int i = 0; i < 5; i++) {
      connections[i] = Mockito.mock(HttpClientConnection.class);
      keepAliveCache.put(routes, connections[i]);
    }

    for (int i = 0; i < 3; i++) {
      Mockito.doReturn(true).when(connections[i]).isStale();
    }

    for (int i = 4; i >= 0; i--) {
      if (i >= 3) {
        Assert.assertNotNull(keepAliveCache.get(routes));
      } else {
        Assert.assertNull(keepAliveCache.get(routes));
        Mockito.verify(connections[i], Mockito.times(1)).close();
      }
    }
  }
}
