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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Invocation Handler for ozone client which dispatches the call to underlying
 * ClientProtocol implementation.
 */
public class OzoneClientInvocationHandler implements InvocationHandler {


  private static final Logger LOG = LoggerFactory.getLogger(OzoneClient.class);
  private final ClientProtocol target;

  /**
   * Constructs OzoneClientInvocationHandler with the proxy.
   * @param target proxy to be used for method invocation.
   */
  public OzoneClientInvocationHandler(ClientProtocol target) {
    this.target = target;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    LOG.trace("Invoking method {} on proxy {}", method, proxy);
    try {
      long startTime = Time.monotonicNow();
      Object result = method.invoke(target, args);
      LOG.debug("Call: {} took {} ms", method,
          Time.monotonicNow() - startTime);
      return result;
    } catch(InvocationTargetException iEx) {
      throw iEx.getCause();
    }
  }
}