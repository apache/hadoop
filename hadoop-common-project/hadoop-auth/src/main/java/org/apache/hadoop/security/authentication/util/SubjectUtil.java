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

package org.apache.hadoop.security.authentication.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * An utility class that adapts the Security Manager and APIs related to it for
 * JDK 8 and above.
 * <p>
 * In JDK 17, the Security Manager and APIs related to it have been deprecated
 * and are subject to removal in a future release. There is no replacement for
 * the Security Manager. See <a href="https://openjdk.org/jeps/411">JEP 411</a>
 * for discussion and alternatives.
 * <p>
 * In JDK 24, the Security Manager has been permanently disabled. See
 * <a href="https://openjdk.org/jeps/486">JEP 486</a> for more information.
 * <p>
 * This is derived from Apache Calcite Avatica, which is derived from the Jetty
 * implementation.
 */
@Private
public final class SubjectUtil {
  private static final MethodHandle CALL_AS = lookupCallAs();
  static final boolean HAS_CALL_AS = CALL_AS != null;
  private static final MethodHandle DO_AS = HAS_CALL_AS ? null : lookupDoAs();
  private static final MethodHandle DO_AS_THROW_EXCEPTION =
      HAS_CALL_AS ? null : lookupDoAsThrowException();
  private static final MethodHandle CURRENT = lookupCurrent();

  private static MethodHandle lookupCallAs() {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      try {
        // Subject.callAs() is available since Java 18.
        return lookup.findStatic(Subject.class, "callAs",
            MethodType.methodType(Object.class, Subject.class, Callable.class));
      } catch (NoSuchMethodException x) {
        return null;
      }
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupDoAs() {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      MethodType signature = MethodType.methodType(
          Object.class, Subject.class, PrivilegedAction.class);
      return lookup.findStatic(Subject.class, "doAs", signature);
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupDoAsThrowException() {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      MethodType signature = MethodType.methodType(
          Object.class, Subject.class, PrivilegedExceptionAction.class);
      return lookup.findStatic(Subject.class, "doAs", signature);
    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupCurrent() {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      // Subject.getSubject(AccessControlContext) is deprecated for removal and
      // replaced by Subject.current().
      // Lookup first the new API, since for Java versions where both exists, the
      // new API delegates to the old API (e.g. Java 18, 19 and 20).
      // Otherwise (e.g. Java 17), lookup the old API.
      return lookup.findStatic(
          Subject.class, "current", MethodType.methodType(Subject.class));
    } catch (NoSuchMethodException e) {
      MethodHandle getContext = lookupGetContext();
      MethodHandle getSubject = lookupGetSubject();
      return MethodHandles.filterReturnValue(getContext, getSubject);
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupGetSubject() {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      Class<?> contextKlass = ClassLoader.getSystemClassLoader()
          .loadClass("java.security.AccessControlContext");
      return lookup.findStatic(Subject.class,
          "getSubject", MethodType.methodType(Subject.class, contextKlass));
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle lookupGetContext() {
    try {
      // Use reflection to work with Java versions that have and don't have
      // AccessController.
      Class<?> controllerKlass = ClassLoader.getSystemClassLoader()
          .loadClass("java.security.AccessController");
      Class<?> contextKlass = ClassLoader.getSystemClassLoader()
          .loadClass("java.security.AccessControlContext");

      MethodHandles.Lookup lookup = MethodHandles.lookup();
      return lookup.findStatic(
          controllerKlass, "getContext", MethodType.methodType(contextKlass));
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Map to Subject.callAs() if available, otherwise maps to Subject.doAs().
   *
   * @param subject the subject this action runs as
   * @param action  the action to run
   * @return the result of the action
   * @param <T> the type of the result
   * @throws NullPointerException if action is null
   * @throws CompletionException if {@code action.call()} throws an exception.
   *      The cause of the {@code CompletionException} is set to the exception
   *      thrown by {@code action.call()}.
   */
  @SuppressWarnings("unchecked")
  public static <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
    Objects.requireNonNull(action);
    if (HAS_CALL_AS) {
      try {
        return (T) CALL_AS.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    } else {
      try {
        return doAs(subject, callableToPrivilegedAction(action));
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }
  }

  /**
   * Map action to a Callable on Java 18 onwards, and delegates to callAs().
   * Call Subject.doAs directly on older JVM.
   * <p>
   * Note: Exception propagation behavior is different since Java 12, it always
   * throw the original exception thrown by action; for lower Java versions,
   * throw a PrivilegedActionException that wraps the original exception when
   * action throw a checked exception.
   *
   * @param subject the subject this action runs as
   * @param action the action to run
   * @return the result of the action
   * @param <T> the type of the result
   * @throws NullPointerException if action is null
   */
  @SuppressWarnings("unchecked")
  public static <T> T doAs(Subject subject, PrivilegedAction<T> action) {
    Objects.requireNonNull(action);
    if (HAS_CALL_AS) {
      try {
        return callAs(subject, privilegedActionToCallable(action));
      } catch (CompletionException ce) {
        Throwable cause = ce.getCause();
        if (cause != null) {
          throw sneakyThrow(cause);
        } else {
          // This should never happen, CompletionException thrown by Subject.callAs
          // should always wrap an exception
          throw ce;
        }
      }
    } else {
      try {
        return (T) DO_AS.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    }
  }

  /**
   * Maps action to a Callable on Java 18 onwards, and delegates to callAs().
   * Call Subject.doAs directly on older JVM.
   *
   * @param subject the subject this action runs as
   * @param action the action to run
   * @return the result of the action
   * @param <T> the type of the result
   * @throws NullPointerException if action is null
   * @throws PrivilegedActionException if {@code action.run()} throws an checked exception.
   *      The cause of the {@code PrivilegedActionException} is set to the exception thrown
   *      by {@code action.run()}.
   */
  @SuppressWarnings("unchecked")
  public static <T> T doAs(
      Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
    Objects.requireNonNull(action);
    if (HAS_CALL_AS) {
      try {
        return callAs(subject, privilegedExceptionActionToCallable(action));
      } catch (CompletionException ce) {
        Throwable cause = ce.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Exception) {
          throw new PrivilegedActionException((Exception) cause);
        } else {
          // This should never happen, CompletionException should only wraps an exception
          throw sneakyThrow(cause);
        }
      }
    } else {
      try {
        return (T) DO_AS_THROW_EXCEPTION.invoke(subject, action);
      } catch (Throwable t) {
        throw sneakyThrow(t);
      }
    }
  }

  /**
   * Maps to Subject.current() if available, otherwise maps to Subject.getSubject().
   *
   * @return the current subject
   */
  public static Subject current() {
    try {
      return (Subject) CURRENT.invoke();
    } catch (Throwable t) {
      throw sneakyThrow(t);
    }
  }

  private static <T> PrivilegedAction<T> callableToPrivilegedAction(
      Callable<T> callable) {
    return () -> {
      try {
        return callable.call();
      } catch (Exception e) {
        throw sneakyThrow(e);
      }
    };
  }

  private static <T> Callable<T> privilegedExceptionActionToCallable(
      PrivilegedExceptionAction<T> action) {
    return action::run;
  }

  private static <T> Callable<T> privilegedActionToCallable(
      PrivilegedAction<T> action) {
    return action::run;
  }

  /**
   * The sneaky throw concept allows the caller to throw any checked exception without
   * defining it explicitly in the method signature.
   * <p>
   * See <a href="https://www.baeldung.com/java-sneaky-throws">"Sneaky Throws" in Java</a>
   * for more details.
   *
   * @param e the exception that will be thrown.
   * @return unreachable, the method always throws an exception before returning
   * @param <E> the thrown exception type, trick the compiler into inferring it as
   *      a {@code RuntimeException} type.
   * @throws E the original exception passes by caller
   */
  @SuppressWarnings("unchecked")
  static <E extends Throwable> RuntimeException sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }

  private SubjectUtil() {
  }
}
