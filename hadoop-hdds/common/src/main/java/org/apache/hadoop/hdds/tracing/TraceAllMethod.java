/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.tracing;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;

/**
 * A Java proxy invocation handler to trace all the methods of the delegate
 * class.
 *
 * @param <T>
 */
public class TraceAllMethod<T> implements InvocationHandler {

  /**
   * Cache for all the method objects of the delegate class.
   */
  private final Map<String, Map<Class<?>[], Method>> methods = new HashMap<>();

  private T delegate;

  private String name;

  public TraceAllMethod(T delegate, String name) {
    this.delegate = delegate;
    this.name = name;
    for (Method method : delegate.getClass().getDeclaredMethods()) {
      if (!methods.containsKey(method.getName())) {
        methods.put(method.getName(), new HashMap<>());
      }
      methods.get(method.getName()).put(method.getParameterTypes(), method);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    Method delegateMethod = findDelegatedMethod(method);
    try (Scope scope = GlobalTracer.get().buildSpan(
        name + "." + method.getName())
        .startActive(true)) {
      try {
        return delegateMethod.invoke(delegate, args);
      } catch (Exception ex) {
        if (ex.getCause() != null) {
          throw ex.getCause();
        } else {
          throw ex;
        }
      }
    }
  }

  private Method findDelegatedMethod(Method method) {
    for (Entry<Class<?>[], Method> entry : methods.get(method.getName())
        .entrySet()) {
      if (Arrays.equals(entry.getKey(), method.getParameterTypes())) {
        return entry.getValue();
      }
    }
    return null;
  }
}
