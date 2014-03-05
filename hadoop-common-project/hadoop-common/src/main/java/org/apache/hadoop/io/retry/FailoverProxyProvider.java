/*
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
package org.apache.hadoop.io.retry;

import java.io.Closeable;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * An implementer of this interface is capable of providing proxy objects for
 * use in IPC communication, and potentially modifying these objects or creating
 * entirely new ones in the event of certain types of failures. The
 * determination of whether or not to fail over is handled by
 * {@link RetryPolicy}.
 */
@InterfaceStability.Evolving
public interface FailoverProxyProvider<T> extends Closeable {
  public static final class ProxyInfo<T> {
    public final T proxy;
    /*
     * The information (e.g., the IP address) of the current proxy object. It
     * provides information for debugging purposes.
     */
    public final String proxyInfo;
    public ProxyInfo(T proxy, String proxyInfo) {
      this.proxy = proxy;
      this.proxyInfo = proxyInfo;
    }
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs.
   * 
   * @return the proxy object to invoke methods upon
   */
  public ProxyInfo<T> getProxy();

  /**
   * Called whenever the associated {@link RetryPolicy} determines that an error
   * warrants failing over.
   * 
   * @param currentProxy
   *          the proxy object which was being used before this failover event
   */
  public void performFailover(T currentProxy);

  /**
   * Return a reference to the interface this provider's proxy objects actually
   * implement. If any of the methods on this interface are annotated as being
   * {@link Idempotent} or {@link AtMostOnce}, then this fact will be passed to
   * the {@link RetryPolicy#shouldRetry(Exception, int, int, boolean)} method on
   * error, for use in determining whether or not failover should be attempted.
   * 
   * @return the interface implemented by the proxy objects returned by
   *         {@link FailoverProxyProvider#getProxy()}
   */
  public Class<T> getInterface();
}