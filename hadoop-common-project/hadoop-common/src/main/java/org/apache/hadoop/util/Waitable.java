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
package org.apache.hadoop.util;

import java.util.concurrent.locks.Condition;

/**
 * Represents an object that you can wait for.
 */
public class Waitable<T> {
  private T val;
  private final Condition cond;

  public Waitable(Condition cond) {
    this.val = null;
    this.cond = cond;
  }

  public T await() throws InterruptedException {
    while (this.val == null) {
      this.cond.await();
    }
    return this.val;
  }

  public void provide(T val) {
    this.val = val;
    this.cond.signalAll();
  }

  public boolean hasVal() {
    return this.val != null;
  }

  public T getVal() {
    return this.val;
  }
}