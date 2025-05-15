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

import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.AuditIntegration;
import org.apache.hadoop.fs.s3a.audit.AuditorFlags;
import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.s3a.audit.OperationAuditorOptions;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.AbstractService;

import static java.util.Objects.requireNonNull;

/**
 * This is a long-lived service which is created in S3A FS initialize
 * (make it fast!) which provides context for tracking operations made to S3.
 * An IOStatisticsStore is passed in -in production this is expected to
 * be the S3AFileSystem instrumentation, which will have the
 * {@code AUDIT_SPAN_START} statistic configured for counting durations.
 */
public abstract class AbstractOperationAuditor extends AbstractService
    implements OperationAuditor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractOperationAuditor.class);

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
   * Should out of span requests be rejected?
   */
  private AtomicBoolean rejectOutOfSpan = new AtomicBoolean(false);

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
   * Audit flags which can be passed down to subclasses.
   */
  private EnumSet<AuditorFlags> auditorFlags;

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
    this.iostatistics = requireNonNull(opts.getIoStatisticsStore());
    init(opts.getConfiguration());
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    setRejectOutOfSpan(AuditIntegration.isRejectOutOfSpan(conf));
    LOG.debug("{}: Out of span operations will be {}",
        getName(),
        isRejectOutOfSpan() ? "rejected" : "ignored");
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

  /**
   * Should out of scope ops be rejected?
   * @return true if out of span calls should be rejected.
   */
  protected boolean isRejectOutOfSpan() {
    return rejectOutOfSpan.get();
  }

  /**
   * Enable/disable out of span rejection.
   * @param rejectOutOfSpan new value.
   */
  protected void setRejectOutOfSpan(boolean rejectOutOfSpan) {
    this.rejectOutOfSpan.set(rejectOutOfSpan);
  }

  /**
   * Update Auditor flags.
   * Calls {@link #auditorFlagsChanged(EnumSet)} after the update.
   * @param flags audit flags.
   */
  @Override
  public void setAuditFlags(final EnumSet<AuditorFlags> flags) {
    auditorFlags = flags;
    auditorFlagsChanged(flags);
  }

  /**
   * Get the current set of auditor flags.
   *
   * @return the current set of auditor flags.
   */
  public EnumSet<AuditorFlags> getAuditorFlags() {
    return auditorFlags;
  }

  /**
   * Notification that the auditor flags have been updated.
   * @param flags audit flags.
   */
  protected void auditorFlagsChanged(EnumSet<AuditorFlags> flags) {
    // if out of band operations are allowed, configuration settings are overridden
    if (flags.contains(AuditorFlags.PermitOutOfBandOperations)) {
      LOG.debug("Out of span operations are required by the stream factory");
      setRejectOutOfSpan(false);
    }
  }

}
