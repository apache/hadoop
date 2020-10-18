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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * A subclass of {@link AbstractStoreOperation} which
 * provides a method {@link #execute()} that may be invoked
 * exactly once.
 * @param <T> return type of executed operation.
 */
public abstract class ExecutingStoreOperation<T>
    extends AbstractStoreOperation {

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * constructor.
   * @param storeContext store context.
   */
  protected ExecutingStoreOperation(final StoreContext storeContext) {
    super(storeContext);
  }

  /**
   * Execute the operation.
   * Subclasses MUST call {@link #executeOnlyOnce()} so as to force
   * the (atomic) re-entrancy check.
   * @return the result.
   * @throws IOException IO problem
   */
  public abstract T execute() throws IOException ;

  /**
   * Check that the operation has not been invoked twice.
   * This is an atomic check.
   * @throws IllegalStateException on a second invocation.
   */
  protected void executeOnlyOnce() {
    Preconditions.checkState(
        !executed.getAndSet(true),
        "Operation attempted twice");
  }

}
