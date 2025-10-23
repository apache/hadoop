/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.util.dynamic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.util.Preconditions.checkState;


/**
 * Dynamic method invocation.
 * Taken from {@code org.apache.parquet.util.DynMethods}.
 */
@InterfaceAudience.LimitedPrivate("testing")
@InterfaceStability.Unstable
public final class DynMethods {

  private static final Logger LOG = LoggerFactory.getLogger(DynMethods.class);

  private DynMethods() {
  }

  /**
   * Convenience wrapper class around {@link Method}.
   * <p>
   * Allows callers to invoke the wrapped method with all Exceptions wrapped by
   * RuntimeException, or with a single Exception catch block.
   */
  public static class UnboundMethod {

    private final Method method;

    private final String name;

    private final int argLength;

    UnboundMethod(Method method, String name) {
      this.method = method;
      this.name = name;
      this.argLength =
          (method == null || method.isVarArgs()) ? -1 : method.getParameterTypes().length;
    }

    @SuppressWarnings("unchecked")
    public <R> R invokeChecked(Object target, Object... args) throws Exception {
      try {
        if (argLength < 0) {
          return (R) method.invoke(target, args);
        } else {
          if (argLength != args.length) {
            LOG.error("expected {} arguments but got {}", argLength, args.length);
          }
          return (R) method.invoke(target, Arrays.copyOfRange(args, 0, argLength));
        }
      } catch (InvocationTargetException e) {
        throwIfInstance(e.getCause(), Exception.class);
        throwIfInstance(e.getCause(), RuntimeException.class);
        throw new RuntimeException(e.getCause());
      }
    }

    public <R> R invoke(Object target, Object... args) {
      try {
        return this.<R>invokeChecked(target, args);
      } catch (Exception e) {
        throwIfInstance(e, RuntimeException.class);
        throw new RuntimeException(e);
      }
    }

    /**
     * Invoke a static method.
     * @param args arguments.
     * @return result.
     * @param <R> type of result.
     */
    public <R> R invokeStatic(Object... args) {
      checkState(isStatic(), "Method is not static %s", toString());
      return invoke(null, args);
    }

    /**
     * Returns this method as a BoundMethod for the given receiver.
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} for this method and the receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     */
    public BoundMethod bind(Object receiver) {
      checkState(!isStatic(), "Cannot bind static method %s",
          method.toGenericString());
      Preconditions.checkArgument(method.getDeclaringClass().isAssignableFrom(receiver.getClass()),
          "Cannot bind %s to instance of %s", method.toGenericString(), receiver.getClass());

      return new BoundMethod(this, receiver);
    }

    /**
     * @return whether the method is a static method
     */
    public boolean isStatic() {
      return Modifier.isStatic(method.getModifiers());
    }

    /**
     * @return whether the method is a noop
     */
    public boolean isNoop() {
      return this == NOOP;
    }

    /**
     * Returns this method as a StaticMethod.
     * @return a {@link StaticMethod} for this method
     * @throws IllegalStateException if the method is not static
     */
    public StaticMethod asStatic() {
      checkState(isStatic(), "Method is not static");
      return new StaticMethod(this);
    }

    public String toString() {
      return "DynMethods.UnboundMethod(name=" + name + " method=" + method.toGenericString() + ")";
    }

    /**
     * Singleton {@link UnboundMethod}, performs no operation and returns null.
     */
    private static final UnboundMethod NOOP = new UnboundMethod(null, "NOOP") {

      @Override
      public <R> R invokeChecked(Object target, Object... args) throws Exception {
        return null;
      }

      @Override
      public BoundMethod bind(Object receiver) {
        return new BoundMethod(this, receiver);
      }

      @Override
      public StaticMethod asStatic() {
        return new StaticMethod(this);
      }

      @Override
      public boolean isStatic() {
        return true;
      }

      @Override
      public String toString() {
        return "DynMethods.UnboundMethod(NOOP)";
      }
    };
  }

  public static final class BoundMethod {

    private final UnboundMethod method;

    private final Object receiver;

    private BoundMethod(UnboundMethod method, Object receiver) {
      this.method = method;
      this.receiver = receiver;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(receiver, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(receiver, args);
    }
  }

  public static final class StaticMethod {

    private final UnboundMethod method;

    private StaticMethod(UnboundMethod method) {
      this.method = method;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(null, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(null, args);
    }
  }

  /**
   * If the given throwable is an instance of E, throw it as an E.
   * @param t an exception instance
   * @param excClass an exception class t may be an instance of
   * @param <E> the type of exception that will be thrown if throwable is an instance
   * @throws E if t is an instance of E
   */
  @SuppressWarnings("unchecked")
  public static <E extends Exception> void throwIfInstance(Throwable t, Class<E> excClass)
      throws E {
    if (excClass.isAssignableFrom(t.getClass())) {
      // the throwable is already an exception, so throw it
      throw (E)t;
    }
  }

  public static final class Builder {

    private final String name;

    private ClassLoader loader = Thread.currentThread().getContextClassLoader();

    private UnboundMethod method = null;

    public Builder(String methodName) {
      this.name = methodName;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     * <p>
     * If not set, the current thread's ClassLoader is used.
     * @param classLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader classLoader) {
      this.loader = classLoader;
      return this;
    }

    /**
     * If no implementation has been found, adds a NOOP method.
     * <p>
     * Note: calls to impl will not match after this method is called!
     * @return this Builder for method chaining
     */
    public Builder orNoop() {
      if (method == null) {
        this.method = UnboundMethod.NOOP;
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder impl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // class not found on supplied classloader.
        LOG.debug("failed to load class {}", className, e);
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     * <p>
     * The name passed to the constructor is the method name used.
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder impl(String className, Class<?>... argClasses) {
      impl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     * @param targetClass the class to check for an implementation
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder impl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new UnboundMethod(targetClass.getMethod(methodName, argClasses), name);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        LOG.debug("failed to load method {} from class {}", methodName, targetClass, e);
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     * <p>
     * The name passed to the constructor is the method name used.
     * @param targetClass the class to check for an implementation
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder impl(Class<?> targetClass, Class<?>... argClasses) {
      impl(targetClass, name, argClasses);
      return this;
    }

    public Builder ctorImpl(Class<?> targetClass, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new DynConstructors.Builder().impl(targetClass, argClasses).buildChecked();
      } catch (NoSuchMethodException e) {
        // not the right implementation
        LOG.debug("failed to load constructor arity {} from class {}", argClasses.length,
            targetClass, e);
      }
      return this;
    }

    public Builder ctorImpl(String className, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new DynConstructors.Builder().impl(className, argClasses).buildChecked();
      } catch (NoSuchMethodException e) {
        // not the right implementation
        LOG.debug("failed to load constructor arity {} from class {}", argClasses.length, className,
            e);
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder hiddenImpl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        hiddenImpl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // class not found on supplied classloader.
        LOG.debug("failed to load class {}", className, e);
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     * <p>
     * The name passed to the constructor is the method name used.
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder hiddenImpl(String className, Class<?>... argClasses) {
      hiddenImpl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     * @param targetClass the class to check for an implementation
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder hiddenImpl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Method hidden = targetClass.getDeclaredMethod(methodName, argClasses);
        AccessController.doPrivileged(new MakeAccessible(hidden));
        this.method = new UnboundMethod(hidden, name);
      } catch (SecurityException | NoSuchMethodException e) {
        // unusable or not the right implementation
        LOG.debug("failed to load method {} from class {}", methodName, targetClass, e);
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     * <p>
     * The name passed to the constructor is the method name used.
     * @param targetClass the class to check for an implementation
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     */
    public Builder hiddenImpl(Class<?> targetClass, Class<?>... argClasses) {
      hiddenImpl(targetClass, name, argClasses);
      return this;
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a
     * NoSuchMethodException if there is none.
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws NoSuchMethodException if no implementation was found
     */
    public UnboundMethod buildChecked() throws NoSuchMethodException {
      if (method != null) {
        return method;
      } else {
        throw new NoSuchMethodException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a
     * RuntimeError if there is none.
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    public UnboundMethod build() {
      if (method != null) {
        return method;
      } else {
        throw new RuntimeException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a
     * NoSuchMethodException if there is none.
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws NoSuchMethodException if no implementation was found
     */
    public BoundMethod buildChecked(Object receiver) throws NoSuchMethodException {
      return buildChecked().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a
     * RuntimeError if there is none.
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws RuntimeException if no implementation was found
     */
    public BoundMethod build(Object receiver) {
      return build().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a
     * NoSuchMethodException if there is none.
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws NoSuchMethodException if no implementation was found
     */
    public StaticMethod buildStaticChecked() throws NoSuchMethodException {
      return buildChecked().asStatic();
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a
     * RuntimeException if there is none.
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws RuntimeException if no implementation was found
     */
    public StaticMethod buildStatic() {
      return build().asStatic();
    }
  }

  private static final class MakeAccessible implements PrivilegedAction<Void> {

    private Method hidden;

    MakeAccessible(Method hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }
}
