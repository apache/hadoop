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

package org.apache.hadoop.fs.s3a.audit.impl;

import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.AbstractService;

/**
 * This is a long-lived service which is created in S3A FS initialize
 * (make it fast!) which provides context for tracking operations made to S3.
 * An IOStatisticsStore is passed in -in production this is expected to
 * be the S3AFileSystem instrumentation, which will have the
 * {@code AUDIT_SPAN_START} statistic configured for counting durations.
 */
public abstract class AbstractOperationAuditor extends AbstractService
    implements OperationAuditor {

  /**
   * Destination for recording statistics, especially duration/count of
   * operations.
   */
  private final IOStatisticsStore iostatistics;

  /**
   * Construct.
   * @param name name
   * @param iostatistics store of statistics.
   */
  protected AbstractOperationAuditor(final String name,
      final IOStatisticsStore iostatistics) {
    super(name);
    this.iostatistics = iostatistics;
  }

  /**
   * Get the IOStatistics Store.
   * @return the IOStatistics store updated with statistics.
   */
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

}
