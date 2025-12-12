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
package org.apache.hadoop.util;

/**
 * A concurrency controller that uses a single lock for all operations.
 *
 * @param <K> The type of the key.
 */
public class SynchronizedGSetController<K> implements GSetConcurrencyController<K> {

  private final Object lock = new Object();

  @Override
  public Object getLock(K key) {
    return lock;
  }

  @Override
  public void correctSize(int size) {
    // do nothing
  }

  @Override
  public void addSize(int deltaSize) {
    // do nothing
  }

  @Override
  public void correctSize() {
    // do nothing
  }

  public static <K> SynchronizedGSetController<K> of() {
    return new SynchronizedGSetController<>();
  }

}
