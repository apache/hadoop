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

package org.apache.hadoop.util.dynamic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Utility methods to assist binding to Hadoop APIs through reflection.
 * Source: {@code org.apache.parquet.hadoop.util.wrapped.io.BindingUtils}.
 */
@InterfaceAudience.LimitedPrivate("testing")
@InterfaceStability.Unstable
public final class BindingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BindingUtils.class);

  private BindingUtils() {}

  /**
   * Load a class by name.
   * @param className classname
   * @return the class or null if it could not be loaded.
   */
  public static Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOG.debug("No class {}", className, e);
      return null;
    }
  }

  /**
   * Load a class by name.
   * @param className classname
   * @return the class.
   * @throws RuntimeException if the class was not found.
   */
  public static Class<?> loadClassSafely(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load a class by name.
   * @param cl classloader to use.
   * @param className classname
   * @return the class or null if it could not be loaded.
   */
  public static Class<?> loadClass(ClassLoader cl, String className) {
    try {
      return cl.loadClass(className);
    } catch (ClassNotFoundException e) {
      LOG.debug("No class {}", className, e);
      return null;
    }
  }


  /**
   * Get an invocation from the source class, which will be unavailable() if
   * the class is null or the method isn't found.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   *
   * @return the method or "unavailable"
   */
  public static <T> DynMethods.UnboundMethod loadInvocation(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    if (source != null) {
      final DynMethods.UnboundMethod m = new DynMethods.Builder(name)
          .impl(source, name, parameterTypes)
          .orNoop()
          .build();
      if (m.isNoop()) {
        // this is a sign of a mismatch between this class's expected
        // signatures and actual ones.
        // log at debug.
        LOG.debug("Failed to load method {} from {}", name, source);
      } else {
        LOG.debug("Found method {} from {}", name, source);
      }
      return m;
    } else {
      return noop(name);
    }
  }

  /**
   * Load a static method from the source class, which will be a noop() if
   * the class is null or the method isn't found.
   * If the class and method are not found, then an {@code IllegalStateException}
   * is raised on the basis that this means that the binding class is broken,
   * rather than missing/out of date.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   *
   * @return the method or a no-op.
   * @throws IllegalStateException if the method is not static.
   */
  public static <T> DynMethods.UnboundMethod loadStaticMethod(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    final DynMethods.UnboundMethod method =
        loadInvocation(source, returnType, name, parameterTypes);
    if (!available(method)) {
      LOG.debug("Method not found: {}", name);
    }
    checkState(method.isStatic(), "Method is not static %s", method);
    return method;
  }

  /**
   * Create a no-op method.
   *
   * @param name method name
   *
   * @return a no-op method.
   */
  public static DynMethods.UnboundMethod noop(final String name) {
    return new DynMethods.Builder(name).orNoop().build();
  }

  /**
   * Given a sequence of methods, verify that they are all available.
   *
   * @param methods methods
   *
   * @return true if they are all implemented
   */
  public static boolean implemented(DynMethods.UnboundMethod... methods) {
    for (DynMethods.UnboundMethod method : methods) {
      if (method.isNoop()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Require a method to be available.
   * @param method method to probe
   * @throws UnsupportedOperationException if the method was not found.
   */
  public static void checkAvailable(DynMethods.UnboundMethod method)
      throws UnsupportedOperationException {
    if (!available(method)) {
      throw new UnsupportedOperationException("Unbound " + method);
    }
  }

  /**
   * Is a method available?
   * @param method method to probe
   * @return true iff the method is found and loaded.
   */
  public static boolean available(DynMethods.UnboundMethod method) {
    return !method.isNoop();
  }

  /**
   * Invoke the supplier, catching any {@code UncheckedIOException} raised,
   * extracting the inner IOException and rethrowing it.
   * @param call call to invoke
   * @return result
   * @param <T> type of result
   * @throws IOException if the call raised an IOException wrapped by an UncheckedIOException.
   */
  public static <T> T extractIOEs(Supplier<T> call) throws IOException {
    try {
      return call.get();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
