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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.util.dynamic.DynMethods.throwIfInstance;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Dynamic constructors.
 * Taken from {@code org.apache.parquet.util.DynConstructors}.
 */
@InterfaceAudience.LimitedPrivate("testing")
@InterfaceStability.Unstable
public class DynConstructors {
  public static final class Ctor<C> extends DynMethods.UnboundMethod {
    private final Constructor<C> ctor;
    private final Class<? extends C> constructed;

    private Ctor(Constructor<C> constructor, Class<? extends C> constructed) {
      super(null, "newInstance");
      this.ctor = constructor;
      this.constructed = constructed;
    }

    public Class<? extends C> getConstructedClass() {
      return constructed;
    }

    public C newInstanceChecked(Object... args) throws Exception {
      try {
        return ctor.newInstance(args);
      } catch (InstantiationException | IllegalAccessException e) {
        throw e;
      } catch (InvocationTargetException e) {
        throwIfInstance(e.getCause(), Exception.class);
        throwIfInstance(e.getCause(), RuntimeException.class);
        throw new RuntimeException(e.getCause());
      }
    }

    public C newInstance(Object... args) {
      try {
        return newInstanceChecked(args);
      } catch (Exception e) {
        throwIfInstance(e, RuntimeException.class);
        throw new RuntimeException(e);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R invoke(Object target, Object... args) {
      checkArgument(target == null, "Invalid call to constructor: target must be null");
      return (R) newInstance(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R invokeChecked(Object target, Object... args) throws Exception {
      checkArgument(target == null, "Invalid call to constructor: target must be null");
      return (R) newInstanceChecked(args);
    }

    @Override
    public DynMethods.BoundMethod bind(Object receiver) {
      throw new IllegalStateException("Cannot bind constructors");
    }

    @Override
    public boolean isStatic() {
      return true;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(constructor=" + ctor + ", class=" + constructed + ")";
    }
  }

  public static class Builder {
    private final Class<?> baseClass;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private Ctor ctor = null;
    private Map<String, Throwable> problems = new HashMap<String, Throwable>();

    public Builder(Class<?> baseClass) {
      this.baseClass = baseClass;
    }

    public Builder() {
      this.baseClass = null;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     * <p>
     * If not set, the current thread's ClassLoader is used.
     *
     * @param value a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader value) {
      this.loader = value;
      return this;
    }

    public Builder impl(String className, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, types);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        // cannot load this implementation
        problems.put(className, e);
      }

      return this;
    }

    public <T> Builder impl(Class<T> targetClass, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        ctor = new Ctor<T>(targetClass.getConstructor(types), targetClass);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        problems.put(methodName(targetClass, types), e);
      }
      return this;
    }

    public Builder hiddenImpl(Class<?>... types) {
      hiddenImpl(baseClass, types);
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder hiddenImpl(String className, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Class targetClass = Class.forName(className, true, loader);
        hiddenImpl(targetClass, types);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        // cannot load this implementation
        problems.put(className, e);
      }
      return this;
    }

    public <T> Builder hiddenImpl(Class<T> targetClass, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Constructor<T> hidden = targetClass.getDeclaredConstructor(types);
        AccessController.doPrivileged(new MakeAccessible(hidden));
        ctor = new Ctor<T>(hidden, targetClass);
      } catch (NoSuchMethodException | SecurityException e) {
        // unusable or not the right implementation
        problems.put(methodName(targetClass, types), e);
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public <C> Ctor<C> buildChecked() throws NoSuchMethodException {
      if (ctor != null) {
        return ctor;
      }
      throw new NoSuchMethodException(
          "Cannot find constructor for " + baseClass + "\n" + formatProblems(problems));
    }

    @SuppressWarnings("unchecked")
    public <C> Ctor<C> build() {
      if (ctor != null) {
        return ctor;
      }
      throw new RuntimeException("Cannot find constructor for " + baseClass
          + "\n" + formatProblems(problems));
    }
  }

  private static final class MakeAccessible implements PrivilegedAction<Void> {
    private Constructor<?> hidden;

    private MakeAccessible(Constructor<?> hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }

  private static String formatProblems(Map<String, Throwable> problems) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, Throwable> problem : problems.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append("\n");
      }
      sb.append("\tMissing ")
          .append(problem.getKey())
          .append(" [")
          .append(problem.getValue().getClass().getName())
          .append(": ")
          .append(problem.getValue().getMessage())
          .append("]");
    }
    return sb.toString();
  }

  private static String methodName(Class<?> targetClass, Class<?>... types) {
    StringBuilder sb = new StringBuilder();
    sb.append(targetClass.getName()).append("(");
    boolean first = true;
    for (Class<?> type : types) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(type.getName());
    }
    sb.append(")");
    return sb.toString();
  }
}
