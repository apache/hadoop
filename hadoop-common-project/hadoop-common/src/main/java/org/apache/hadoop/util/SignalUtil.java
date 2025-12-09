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

  static final Class<?> JDK_SIGNAL_CLAZZ =
      BindingUtils.loadClassSafely("sun.misc.Signal");
  static final Class<?> JDK_SIGNAL_HANDLER_CLAZZ =
      BindingUtils.loadClassSafely("sun.misc.SignalHandler");

  static final DynConstructors.Ctor<?> JDK_SIGNAL_CTOR =
      new DynConstructors.Builder()
          .impl(JDK_SIGNAL_CLAZZ, String.class)
          .build();

  static final DynMethods.StaticMethod JDK_SIGNAL_HANDLE_STATIC_METHOD =
      new DynMethods.Builder("handle")
          .impl(JDK_SIGNAL_CLAZZ, JDK_SIGNAL_CLAZZ, JDK_SIGNAL_HANDLER_CLAZZ)
          .buildStatic();

  static final DynMethods.StaticMethod JDK_SIGNAL_RAISE_STATIC_METHOD =
      new DynMethods.Builder("raise")
          .impl(JDK_SIGNAL_CLAZZ, JDK_SIGNAL_CLAZZ)
          .buildStatic();

  static final DynMethods.UnboundMethod JDK_SIGNAL_HANDLER_HANDLE_UNBOUND_METHOD =
      new DynMethods.Builder("handle")
          .impl(JDK_SIGNAL_HANDLER_CLAZZ, JDK_SIGNAL_CLAZZ)
          .build();

  @InterfaceAudience.Private
  public static class Signal {
    private static final DynMethods.UnboundMethod GET_NUMBER_UNBOUND_METHOD =
        new DynMethods.Builder("getNumber").impl(JDK_SIGNAL_CLAZZ).build();

    private static final DynMethods.UnboundMethod GET_NAME_UNBOUND_METHOD =
        new DynMethods.Builder("getName").impl(JDK_SIGNAL_CLAZZ).build();

    private final Object/* sun.misc.Signal */ delegate;
    private final DynMethods.BoundMethod getNumberMethod;
    private final DynMethods.BoundMethod getNameMethod;

    public Signal(String name) {
      Preconditions.checkNotNull(name);
      this.delegate = JDK_SIGNAL_CTOR.newInstance(name);
      this.getNumberMethod = GET_NUMBER_UNBOUND_METHOD.bind(delegate);
      this.getNameMethod = GET_NAME_UNBOUND_METHOD.bind(delegate);
    }

    public Signal(Object delegate) {
      Preconditions.checkArgument(JDK_SIGNAL_CLAZZ.isInstance(delegate),
          String.format("Expected class is '%s', but actual class is '%s'",
              JDK_SIGNAL_CLAZZ.getName(), delegate.getClass().getName()));
      this.delegate = delegate;
      this.getNumberMethod = GET_NUMBER_UNBOUND_METHOD.bind(delegate);
      this.getNameMethod = GET_NAME_UNBOUND_METHOD.bind(delegate);
    }

    public int getNumber() {
      return getNumberMethod.invoke();
    }

    public String getName() {
      return getNameMethod.invoke();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof Signal) {
        return delegate.equals(((Signal)obj).delegate);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }

  @InterfaceAudience.Private
  public interface Handler {
    void handle(Signal sig);
  }

  static class JdkSignalHandlerImpl implements Handler {

    private final Object/* sun.misc.SignalHandler */ delegate;
    private final DynMethods.BoundMethod jdkSignalHandlerHandleMethod;

    JdkSignalHandlerImpl(Handler handler) {
      this.delegate = Proxy.newProxyInstance(
          getClass().getClassLoader(),
          new Class<?>[] {JDK_SIGNAL_HANDLER_CLAZZ},
          (proxyObj, method, args) -> {
            if ("handle".equals(method.getName()) && args.length == 1
                && JDK_SIGNAL_CLAZZ.isInstance(args[0])) {
              handler.handle(new Signal(args[0]));
              return null;
            } else {
              Method delegateMethod = handler.getClass().getMethod(
                  method.getName(), method.getParameterTypes());
              return delegateMethod.invoke(handler, args);
            }
          }
      );
      this.jdkSignalHandlerHandleMethod = JDK_SIGNAL_HANDLER_HANDLE_UNBOUND_METHOD.bind(delegate);
    }

    JdkSignalHandlerImpl(Object delegate) {
      Preconditions.checkArgument(JDK_SIGNAL_HANDLER_CLAZZ.isInstance(delegate),
          String.format("Expected class is '%s', but actual class is '%s'",
              JDK_SIGNAL_HANDLER_CLAZZ.getName(), delegate.getClass().getName()));
      this.delegate = delegate;
      this.jdkSignalHandlerHandleMethod = JDK_SIGNAL_HANDLER_HANDLE_UNBOUND_METHOD.bind(delegate);
    }

    @Override
    public void handle(Signal sig) {
      jdkSignalHandlerHandleMethod.invoke(sig.delegate);
    }
  }

  public static Handler handle(Signal sig, Handler handler) {
    Object preHandle = JDK_SIGNAL_HANDLE_STATIC_METHOD.invoke(
        sig.delegate, new JdkSignalHandlerImpl(handler).delegate);
    return new JdkSignalHandlerImpl(preHandle);
  }

  public static void raise(Signal sig) throws IllegalArgumentException {
    JDK_SIGNAL_RAISE_STATIC_METHOD.invoke(sig.delegate);
  }
}
