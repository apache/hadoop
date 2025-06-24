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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.util.dynamic.BindingUtils;
import org.apache.hadoop.util.dynamic.DynConstructors;
import org.apache.hadoop.util.dynamic.DynMethods;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

@InterfaceAudience.Private
public class SignalUtil {

  static final Class<?> jdkSignalClazz =
      BindingUtils.loadClassSafely("sun.misc.Signal");
  static final Class<?> jdkSignalHandlerClazz =
      BindingUtils.loadClassSafely("sun.misc.SignalHandler");

  static DynConstructors.Ctor<?> jdkSignalCtor =
      new DynConstructors.Builder()
          .impl(jdkSignalClazz, String.class)
          .build();

  static DynMethods.StaticMethod jdkSignalHandleStaticMethod =
      new DynMethods.Builder("handle")
          .impl(jdkSignalClazz, jdkSignalClazz, jdkSignalHandlerClazz)
          .buildStatic();

  static DynMethods.StaticMethod jdkSignalRaiseStaticMethod =
      new DynMethods.Builder("raise")
          .impl(jdkSignalClazz, jdkSignalClazz)
          .buildStatic();

  static DynMethods.UnboundMethod jdkSignalHandlerHandleUnboundMethod =
      new DynMethods.Builder("handle")
          .impl(jdkSignalHandlerClazz, jdkSignalClazz)
          .build();

  @InterfaceAudience.Private
  public static class Signal {
    private static final DynMethods.UnboundMethod getNumberUnboundMethod =
        new DynMethods.Builder("getNumber").impl(jdkSignalClazz).build();

    private static final DynMethods.UnboundMethod getNameUnboundMethod =
        new DynMethods.Builder("getName").impl(jdkSignalClazz).build();

    private final Object delegate;
    private final DynMethods.BoundMethod getNumberMethod;
    private final DynMethods.BoundMethod getNameMethod;

    public Signal(String name) {
      Preconditions.checkNotNull(name);
      this.delegate = jdkSignalCtor.newInstance(name);
      this.getNumberMethod = getNumberUnboundMethod.bind(delegate);
      this.getNameMethod = getNameUnboundMethod.bind(delegate);
    }

    public Signal(Object delegate) {
      Preconditions.checkArgument(jdkSignalClazz.isInstance(delegate),
          String.format("Expected class is '%s', but actual class is '%s'",
              jdkSignalClazz.getName(), delegate.getClass().getName()));
      this.delegate = delegate;
      this.getNumberMethod = getNumberUnboundMethod.bind(delegate);
      this.getNameMethod = getNameUnboundMethod.bind(delegate);
    }

    public int getNumber() {
      return getNumberMethod.invoke();
    }

    public String getName() {
      return getNameMethod.invoke();
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof Signal) {
        return delegate.equals(((Signal)obj).delegate);
      }
      return false;
    }

    public int hashCode() {
      return delegate.hashCode();
    }

    public String toString() {
      return delegate.toString();
    }
  }

  @InterfaceAudience.Private
  public interface Handler {
    void handle(Signal sig);
  }

  static class JdkSignalHandlerImpl implements Handler {

    private final Object delegate;
    private final DynMethods.BoundMethod jdkSignalHandlerHandleMethod;

    JdkSignalHandlerImpl(Handler handler) {
      this.delegate = Proxy.newProxyInstance(
          getClass().getClassLoader(),
          new Class<?>[] { jdkSignalHandlerClazz },
          (proxyObj, method, args) -> {
            if ("handle".equals(method.getName()) && args.length == 1 && jdkSignalClazz.isInstance(args[0])) {
              handler.handle(new Signal(args[0]));
              return null;
            } else {
              Method delegateMethod = Handler.class.getMethod(method.getName(), method.getParameterTypes());
              return delegateMethod.invoke(handler, args);
            }
          }
      );
      this.jdkSignalHandlerHandleMethod = jdkSignalHandlerHandleUnboundMethod.bind(delegate);
    }

    JdkSignalHandlerImpl(Object delegate) {
      Preconditions.checkArgument(jdkSignalHandlerClazz.isInstance(delegate),
          String.format("Expected class is '%s', but actual class is '%s'",
              jdkSignalHandlerClazz.getName(), delegate.getClass().getName()));
      this.delegate = delegate;
      this.jdkSignalHandlerHandleMethod = jdkSignalHandlerHandleUnboundMethod.bind(delegate);
    }

    @Override
    public void handle(Signal sig) {
      jdkSignalHandlerHandleMethod.invoke(sig.delegate);
    }
  }

  public static Handler handle(Signal sig, Handler handler) {
    Object preHandle = jdkSignalHandleStaticMethod.invoke(
        sig.delegate, new JdkSignalHandlerImpl(handler).delegate);
    return new JdkSignalHandlerImpl(preHandle);
  }

  public static void raise(Signal sig) throws IllegalArgumentException {
    jdkSignalRaiseStaticMethod.invoke(sig.delegate);
  }
}
