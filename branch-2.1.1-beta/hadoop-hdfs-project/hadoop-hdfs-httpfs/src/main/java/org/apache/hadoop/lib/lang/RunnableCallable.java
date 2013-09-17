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

package org.apache.hadoop.lib.lang;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.util.Check;

import java.util.concurrent.Callable;

/**
 * Adapter class that allows <code>Runnable</code>s and <code>Callable</code>s to
 * be treated as the other.
 */
@InterfaceAudience.Private
public class RunnableCallable implements Callable<Void>, Runnable {
  private Runnable runnable;
  private Callable<?> callable;

  /**
   * Constructor that takes a runnable.
   *
   * @param runnable runnable.
   */
  public RunnableCallable(Runnable runnable) {
    this.runnable = Check.notNull(runnable, "runnable");
  }

  /**
   * Constructor that takes a callable.
   *
   * @param callable callable.
   */
  public RunnableCallable(Callable<?> callable) {
    this.callable = Check.notNull(callable, "callable");
  }

  /**
   * Invokes the wrapped callable/runnable as a callable.
   *
   * @return void
   *
   * @throws Exception thrown by the wrapped callable/runnable invocation.
   */
  @Override
  public Void call() throws Exception {
    if (runnable != null) {
      runnable.run();
    } else {
      callable.call();
    }
    return null;
  }

  /**
   * Invokes the wrapped callable/runnable as a runnable.
   *
   * @throws RuntimeException thrown by the wrapped callable/runnable invocation.
   */
  @Override
  public void run() {
    if (runnable != null) {
      runnable.run();
    } else {
      try {
        callable.call();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Returns the class name of the wrapper callable/runnable.
   *
   * @return the class name of the wrapper callable/runnable.
   */
  @Override
  public String toString() {
    return (runnable != null) ? runnable.getClass().getSimpleName() : callable.getClass().getSimpleName();
  }
}
