/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.Client.ConnectionId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class used to combine two protocol proxies.
 * See {@link #combine(Class, Object...)}.
 */
public final class ProxyCombiner {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProxyCombiner.class);

  private ProxyCombiner() { }

  /**
   * Combine two or more proxies which together comprise a single proxy
   * interface. This can be used for a protocol interface which {@code extends}
   * multiple other protocol interfaces. The returned proxy will implement
   * all of the methods of the combined proxy interface, delegating calls
   * to which proxy implements that method. If multiple proxies implement the
   * same method, the first in the list will be used for delegation.
   *
   * <p/>This will check that every method on the combined interface is
   * implemented by at least one of the supplied proxy objects.
   *
   * @param combinedProxyInterface The interface of the combined proxy.
   * @param proxies The proxies which should be used as delegates.
   * @param <T> The type of the proxy that will be returned.
   * @return The combined proxy.
   */
  @SuppressWarnings("unchecked")
  public static <T> T combine(Class<T> combinedProxyInterface,
      Object... proxies) {
    methodLoop:
    for (Method m : combinedProxyInterface.getMethods()) {
      for (Object proxy : proxies) {
        try {
          proxy.getClass().getMethod(m.getName(), m.getParameterTypes());
          continue methodLoop; // go to the next method
        } catch (NoSuchMethodException nsme) {
          // Continue to try the next proxy
        }
      }
      throw new IllegalStateException("The proxies specified for "
          + combinedProxyInterface + " do not cover method " + m);
    }

    InvocationHandler handler =
        new CombinedProxyInvocationHandler(combinedProxyInterface, proxies);
    return (T) Proxy.newProxyInstance(combinedProxyInterface.getClassLoader(),
        new Class[] {combinedProxyInterface}, handler);
  }

  private static final class CombinedProxyInvocationHandler
      implements RpcInvocationHandler {

    private final Class<?> proxyInterface;
    private final Object[] proxies;

    private CombinedProxyInvocationHandler(Class<?> proxyInterface,
        Object[] proxies) {
      this.proxyInterface = proxyInterface;
      this.proxies = proxies;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      Exception lastException = null;
      for (Object underlyingProxy : proxies) {
        try {
          return method.invoke(underlyingProxy, args);
        } catch (IllegalAccessException|IllegalArgumentException e) {
          lastException = e;
        } catch (InvocationTargetException ite) {
          throw ite.getCause();
        }
      }
      // This shouldn't happen since the method coverage was verified in build()
      LOG.error("BUG: Method {} was unable to be found on any of the "
          + "underlying proxies for {}", method, proxy.getClass());
      throw new IllegalArgumentException("Method " + method + " not supported",
          lastException);
    }

    /**
     * Since this is incapable of returning multiple connection IDs, simply
     * return the first one. In most cases, the connection ID should be the same
     * for all proxies.
     */
    @Override
    public ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(proxies[0]);
    }

    @Override
    public String toString() {
      return "CombinedProxy[" + proxyInterface.getSimpleName() + "]["
          + Joiner.on(",").join(proxies) + "]";
    }

    @Override
    public void close() throws IOException {
      MultipleIOException.Builder exceptionBuilder =
          new MultipleIOException.Builder();
      for (Object proxy : proxies) {
        if (proxy instanceof Closeable) {
          try {
            ((Closeable) proxy).close();
          } catch (IOException ioe) {
            exceptionBuilder.add(ioe);
          }
        }
      }
      if (!exceptionBuilder.isEmpty()) {
        throw exceptionBuilder.build();
      }
    }
  }
}
