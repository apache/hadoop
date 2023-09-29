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

package org.apache.hadoop.fs.s3a.audit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * Options for the {@link OperationAuditor}.
 * Done as a builder and passed in so
 * that if it is extended, external auditors will still link.
 */
public final class OperationAuditorOptions {

  private Configuration configuration;
  private IOStatisticsStore ioStatisticsStore;


  private OperationAuditorOptions() {
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public OperationAuditorOptions withConfiguration(final Configuration value) {
    configuration = value;
    return this;
  }

  public IOStatisticsStore getIoStatisticsStore() {
    return ioStatisticsStore;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public OperationAuditorOptions withIoStatisticsStore(
      final IOStatisticsStore value) {
    ioStatisticsStore = value;
    return this;
  }

  /**
   * Create one.
   * @return a new option instance
   */
  public static OperationAuditorOptions builder() {
    return new OperationAuditorOptions();
  }
}
