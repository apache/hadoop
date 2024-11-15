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
package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** For handling customized {@link Callback}. */
public interface CustomizedCallbackHandler {
  Logger LOG = LoggerFactory.getLogger(CustomizedCallbackHandler.class);

  class Cache {
    private static final Map<String, CustomizedCallbackHandler> MAP = new HashMap<>();

    private static synchronized CustomizedCallbackHandler getSynchronously(
        String key, Configuration conf) {
      //check again synchronously
      final CustomizedCallbackHandler cached = MAP.get(key);
      if (cached != null) {
        return cached; //cache hit
      }

      //cache miss
      final Class<?> clazz = conf.getClass(key, DefaultHandler.class);
      LOG.info("{} = {}", key, clazz);
      if (clazz == DefaultHandler.class) {
        return DefaultHandler.INSTANCE;
      }

      final Object created;
      try {
        created = clazz.newInstance();
      } catch (Exception e) {
        LOG.warn("Failed to create a new instance of {}, fallback to {}",
            clazz, DefaultHandler.class, e);
        return DefaultHandler.INSTANCE;
      }

      final CustomizedCallbackHandler handler = created instanceof CustomizedCallbackHandler ?
          (CustomizedCallbackHandler) created : CustomizedCallbackHandler.delegate(created);
      MAP.put(key, handler);
      return handler;
    }

    private static CustomizedCallbackHandler get(String key, Configuration conf) {
      final CustomizedCallbackHandler cached = MAP.get(key);
      return cached != null ? cached : getSynchronously(key, conf);
    }

    public static synchronized void clear() {
      MAP.clear();
    }

    private Cache() { }
  }

  class DefaultHandler implements CustomizedCallbackHandler {
    private static final DefaultHandler INSTANCE = new DefaultHandler();

    @Override
    public void handleCallbacks(List<Callback> callbacks, String username, char[] password)
        throws UnsupportedCallbackException {
      if (!callbacks.isEmpty()) {
        final Callback cb = callbacks.get(0);
        throw new UnsupportedCallbackException(callbacks.get(0),
            "Unsupported callback: " + (cb == null ? null : cb.getClass()));
      }
    }
  }

  static CustomizedCallbackHandler delegate(Object delegated) {
    final String methodName = "handleCallbacks";
    final Class<?> clazz = delegated.getClass();
    final Method method;
    try {
      method = clazz.getMethod(methodName, List.class, String.class, char[].class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Failed to get method " + methodName + " from " + clazz, e);
    }

    return (callbacks, name, password) -> {
      try {
        method.invoke(delegated, callbacks, name, password);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IOException("Failed to invoke " + method, e);
      }
    };
  }

  static CustomizedCallbackHandler get(String key, Configuration conf) {
    return Cache.get(key, conf);
  }

  void handleCallbacks(List<Callback> callbacks, String name, char[] password)
      throws UnsupportedCallbackException, IOException;
}
