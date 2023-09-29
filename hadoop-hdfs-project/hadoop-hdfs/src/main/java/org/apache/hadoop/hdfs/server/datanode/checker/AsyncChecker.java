/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.checker;

import java.util.Optional;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A class that can be used to schedule an asynchronous check on a given
 * {@link Checkable}. If the check is successfully scheduled then a
 * {@link ListenableFuture} is returned.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AsyncChecker<K, V> {

  /**
   * Schedule an asynchronous check for the given object.
   *
   * @param target object to be checked.
   *
   * @param context the interpretation of the context depends on the
   *                target.
   *
   * @return returns a {@link Optional of ListenableFuture} that can be used to
   *         retrieve the result of the asynchronous check.
   */
  Optional<ListenableFuture<V>> schedule(Checkable<K, V> target, K context);

  /**
   * Cancel all executing checks and wait for them to complete.
   * First attempts a graceful cancellation, then cancels forcefully.
   * Waits for the supplied timeout after both attempts.
   *
   * See {@link ExecutorService#awaitTermination} for a description of
   * the parameters.
   *
   * @throws InterruptedException
   */
  void shutdownAndWait(long timeout, TimeUnit timeUnit)
      throws InterruptedException;
}
