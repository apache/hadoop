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
package org.apache.hadoop.test;

import static org.junit.Assert.fail;

import java.text.MessageFormat;

import org.apache.hadoop.util.Time;
import org.junit.Rule;
import org.junit.rules.MethodRule;

public abstract class HTestCase {

  public static final String TEST_WAITFOR_RATIO_PROP = "test.waitfor.ratio";

  static {
    SysPropsForTestsLoader.init();
  }

  private static float WAITFOR_RATIO_DEFAULT = Float.parseFloat(System.getProperty(TEST_WAITFOR_RATIO_PROP, "1"));

  private float waitForRatio = WAITFOR_RATIO_DEFAULT;

  @Rule
  public MethodRule testDir = new TestDirHelper();

  @Rule
  public MethodRule jettyTestHelper = new TestJettyHelper();

  @Rule
  public MethodRule exceptionHelper = new TestExceptionHelper();

  /**
   * Sets the 'wait for ratio' used in the {@link #sleep(long)},
   * {@link #waitFor(int, Predicate)} and
   * {@link #waitFor(int, boolean, Predicate)} method for the current
   * test class.
   * <p/>
   * This is useful when running tests in slow machine for tests
   * that are time sensitive.
   *
   * @param ratio the 'wait for ratio' to set.
   */
  protected void setWaitForRatio(float ratio) {
    waitForRatio = ratio;
  }

  /*
   * Returns the 'wait for ratio' used in the {@link #sleep(long)},
   * {@link #waitFor(int, Predicate)} and
   * {@link #waitFor(int, boolean, Predicate)} methods for the current
   * test class.
   * <p/>
   * This is useful when running tests in slow machine for tests
   * that are time sensitive.
   * <p/>
   * The default value is obtained from the Java System property
   * <code>test.wait.for.ratio</code> which defaults to <code>1</code>.
   *
   * @return the 'wait for ratio' for the current test class.
   */
  protected float getWaitForRatio() {
    return waitForRatio;
  }

  /**
   * A predicate 'closure' used by the {@link #waitFor(int, Predicate)} and
   * {@link #waitFor(int, boolean, Predicate)} methods.
   */
  public static interface Predicate {

    /**
     * Perform a predicate evaluation.
     *
     * @return the boolean result of the evaluation.
     *
     * @throws Exception thrown if the predicate evaluation could not evaluate.
     */
    public boolean evaluate() throws Exception;

  }

  /**
   * Makes the current thread sleep for the specified number of milliseconds.
   * <p/>
   * The sleep time is multiplied by the {@link #getWaitForRatio()}.
   *
   * @param time the number of milliseconds to sleep.
   */
  protected void sleep(long time) {
    try {
      Thread.sleep((long) (getWaitForRatio() * time));
    } catch (InterruptedException ex) {
      System.err.println(MessageFormat.format("Sleep interrupted, {0}", ex.toString()));
    }
  }

  /**
   * Waits up to the specified timeout for the given {@link Predicate} to
   * become <code>true</code>, failing the test if the timeout is reached
   * and the Predicate is still <code>false</code>.
   * <p/>
   * The timeout time is multiplied by the {@link #getWaitForRatio()}.
   *
   * @param timeout the timeout in milliseconds to wait for the predicate.
   * @param predicate the predicate ot evaluate.
   *
   * @return the effective wait, in milli-seconds until the predicate become
   *         <code>true</code>.
   */
  protected long waitFor(int timeout, Predicate predicate) {
    return waitFor(timeout, false, predicate);
  }

  /**
   * Waits up to the specified timeout for the given {@link Predicate} to
   * become <code>true</code>.
   * <p/>
   * The timeout time is multiplied by the {@link #getWaitForRatio()}.
   *
   * @param timeout the timeout in milliseconds to wait for the predicate.
   * @param failIfTimeout indicates if the test should be failed if the
   * predicate times out.
   * @param predicate the predicate ot evaluate.
   *
   * @return the effective wait, in milli-seconds until the predicate become
   *         <code>true</code> or <code>-1</code> if the predicate did not evaluate
   *         to <code>true</code>.
   */
  protected long waitFor(int timeout, boolean failIfTimeout, Predicate predicate) {
    long started = Time.now();
    long mustEnd = Time.now() + (long) (getWaitForRatio() * timeout);
    long lastEcho = 0;
    try {
      long waiting = mustEnd - Time.now();
      System.out.println(MessageFormat.format("Waiting up to [{0}] msec", waiting));
      boolean eval;
      while (!(eval = predicate.evaluate()) && Time.now() < mustEnd) {
        if ((Time.now() - lastEcho) > 5000) {
          waiting = mustEnd - Time.now();
          System.out.println(MessageFormat.format("Waiting up to [{0}] msec", waiting));
          lastEcho = Time.now();
        }
        Thread.sleep(100);
      }
      if (!eval) {
        if (failIfTimeout) {
          fail(MessageFormat.format("Waiting timed out after [{0}] msec", timeout));
        } else {
          System.out.println(MessageFormat.format("Waiting timed out after [{0}] msec", timeout));
        }
      }
      return (eval) ? Time.now() - started : -1;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}

