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
package org.apache.hadoop.fi;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Test Utilities */
public class FiTestUtil {
  /** Logging */
  public static final Log LOG = LogFactory.getLog(FiTestUtil.class);

  /** Random source */
  public static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    protected Random initialValue() {
      final Random r = new Random();
      final long seed = r.nextLong();
      LOG.info(Thread.currentThread() + ": seed=" + seed);
      r.setSeed(seed);
      return r;
    }
  };

  /**
   * Return a random integer uniformly distributed in the interval [min,max).
   * Assume max - min <= Integer.MAX_VALUE.
   */
  public static long nextRandomLong(final long min, final long max) {
    final long d = max - min;
    if (d <= 0 || d > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "d <= 0 || d > Integer.MAX_VALUE, min=" + min + ", max=" + max);
    }
    return d == 1? min: min + RANDOM.get().nextInt((int)d);
  }

  /** Return the method name of the callee. */
  public static String getMethodName() {
    final StackTraceElement[] s = Thread.currentThread().getStackTrace();
    return s[s.length > 2? 2: s.length - 1].getMethodName();
  }

  /**
   * Sleep.
   * If there is an InterruptedException, re-throw it as a RuntimeException.
   */
  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sleep a random number of milliseconds over the interval [min, max).
   * If there is an InterruptedException, re-throw it as a RuntimeException.
   */
  public static void sleep(final long min, final long max) {
    final long n = nextRandomLong(min, max);
    LOG.info(Thread.currentThread().getName() + " sleeps for " + n  +"ms");
    if (n > 0) {
      sleep(n);
    }
  }

  /** Action interface */
  public static interface Action<T> {
    /** Run the action with the parameter. */
    public void run(T parameter) throws IOException;
  }

  /** An ActionContainer contains at most one action. */
  public static class ActionContainer<T> {
    private Action<T> action;

    /** Create an empty container. */
    public ActionContainer() {}

    /** Set action. */
    public void set(Action<T> a) {action = a;}

    /** Run the action if it exists. */
    public void run(T obj) throws IOException {
      if (action != null) {
        action.run(obj);
      }
    }
  }

  /** Constraint interface */
  public static interface Constraint {
    /** Is this constraint satisfied? */
    public boolean isSatisfied();
  }

  /** Counting down, the constraint is satisfied if the count is zero. */
  public static class CountdownConstraint implements Constraint {
    private int count;

    /** Initialize the count. */
    public CountdownConstraint(int count) {
      if (count < 0) {
        throw new IllegalArgumentException(count + " = count < 0");
      }
      this.count = count;
    }

    /** Counting down, the constraint is satisfied if the count is zero. */
    public boolean isSatisfied() {
      if (count > 0) {
        count--;
        return false;
      }
      return true;
    }
  }
  
  /** An action is fired if all the constraints are satisfied. */
  public static class ConstraintSatisfactionAction<T> implements Action<T> {
    private final Action<T> action;
    private final Constraint[] constraints;
    
    /** Constructor */
    public ConstraintSatisfactionAction(
        Action<T> action, Constraint... constraints) {
      this.action = action;
      this.constraints = constraints;
    }

    /**
     * Fire the action if all the constraints are satisfied.
     * Short-circuit-and is used. 
     */
    @Override
    public final void run(T parameter) throws IOException {
      for(Constraint c : constraints) {
        if (!c.isSatisfied()) {
          return;
        }
      }

      //all constraints are satisfied, fire the action
      action.run(parameter);
    }
  }
}