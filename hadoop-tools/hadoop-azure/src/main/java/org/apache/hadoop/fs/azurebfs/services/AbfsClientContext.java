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

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Class to hold extra configurations for AbfsClient and further classes
 * inside AbfsClient.
 */
public class AbfsClientContext {

  private ExponentialRetryPolicy exponentialRetryPolicy;
  private AbfsPerfTracker abfsPerfTracker;
  private AbfsCounters abfsCounters;

  public AbfsClientContext withExponentialRetryPolicy(
      final ExponentialRetryPolicy exponentialRetryPolicy) {
    this.exponentialRetryPolicy = exponentialRetryPolicy;
    return this;
  }

  public AbfsClientContext withAbfsPerfTracker(
      final AbfsPerfTracker abfsPerfTracker) {
    this.abfsPerfTracker = abfsPerfTracker;
    return this;
  }

  public AbfsClientContext withAbfsCounters(final AbfsCounters abfsCounters) {
    this.abfsCounters = abfsCounters;
    return this;
  }

  /**
   * Build the context and get the instance with the properties selected.
   *
   * @return an instance of AbfsClientContext.
   */
  public AbfsClientContext build() {
    return this;
  }

  public ExponentialRetryPolicy getExponentialRetryPolicy() {
    return exponentialRetryPolicy;
  }

  public AbfsPerfTracker getAbfsPerfTracker() {
    return abfsPerfTracker;
  }

  public AbfsCounters getAbfsCounters() {
    return abfsCounters;
  }
}
