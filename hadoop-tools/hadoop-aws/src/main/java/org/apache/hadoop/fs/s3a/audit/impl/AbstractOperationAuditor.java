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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.s3a.audit.OperationAuditorOptions;
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
   * Base of IDs is a UUID.
   */
  public static final String BASE = UUID.randomUUID().toString();

  /**
   * Counter to create unique auditor IDs.
   */
  private static final AtomicLong SPAN_ID_COUNTER = new AtomicLong(1);

  /**
   * Destination for recording statistics, especially duration/count of
   * operations.
   * Set in {@link #init(OperationAuditorOptions)}.
   */
  private IOStatisticsStore iostatistics;

  /**
   * Options: set in {@link #init(OperationAuditorOptions)}.
   */
  private OperationAuditorOptions options;

  /**
   * Auditor ID as a UUID.
   */
  private final UUID auditorUUID = UUID.randomUUID();

  /**
   * ID of the auditor, which becomes that of the filesystem
   * in request contexts.
   */
  private final String auditorID = auditorUUID.toString();

  /**
   * Construct.
   * @param name name
   *
   */
  protected AbstractOperationAuditor(final String name) {
    super(name);
  }

  /**
   * Sets the IOStats and then calls init().
   * @param opts options to initialize with.
   */
  @Override
  public void init(final OperationAuditorOptions opts) {
    this.options = opts;
    this.iostatistics = opts.getIoStatisticsStore();
    init(opts.getConfiguration());
  }

  @Override
  public String getAuditorId() {
    return auditorID;
  }

  /**
   * Get the IOStatistics Store.
   * @return the IOStatistics store updated with statistics.
   */
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * Get the options this auditor was initialized with.
   * @return options.
   */
  protected OperationAuditorOptions getOptions() {
    return options;
  }

  /**
   * Create a span ID.
   * @return a unique span ID.
   */
  protected final String createSpanID() {
    return String.format("%s-%08d",
        auditorID, SPAN_ID_COUNTER.incrementAndGet());
  }
}
