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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer.isRequestNotAlwaysInSpan;
import static org.apache.hadoop.fs.s3a.audit.AuditConstants.PRINCIPAL;
import static org.apache.hadoop.fs.s3a.audit.CommonAuditContext.PROCESS_ID;
import static org.apache.hadoop.fs.s3a.audit.CommonAuditContext.currentContext;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.extractJobID;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.HEADER_REFERRER;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;

/**
 * The LoggingAuditor logs operations at DEBUG (in SDK Request) and
 * in span lifecycle and S3 request class construction at TRACE.
 * The context information is added as the HTTP referrer.
 */
@InterfaceAudience.Private
public final class LoggingAuditor
    extends AbstractOperationAuditor {

  /**
   * What to look for in logs for ops outside any audit.
   * {@value}.
   */
  public static final String UNAUDITED_OPERATION = "unaudited operation";

  /**
   * This is where the context gets logged to.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(LoggingAuditor.class);

  /**
   * Counter for next operation in this service.
   * Initial value is what will be used for the span ID when there
   * is no active request span.
   */
  private final AtomicLong nextOperationId = new AtomicLong(1);

  /**
   * Some basic analysis for the logs.
   */
  private final AWSRequestAnalyzer analyzer = new AWSRequestAnalyzer();

  /**
   * Default span to use when there is no other.
   */
  private final AuditSpan warningSpan;

  /**
   * Prefix built up to use in log messages.
   */
  private final String processID;

  /**
   * Should out of scope ops be rejected?
   */
  private boolean rejectOutOfSpan;

  /**
   * Map of attributes which will be added to all operations.
   */
  private final Map<String, String> attributes = new HashMap<>();

  /**
   * UGI principal at time of creation.
   * This is mapped into the common context if it is not already set there
   * when a span is created.
   */
  private final String principal;

  /**
   * Create an operation ID. The nature of it should be opaque.
   * @return an ID for the constructor.
   */
  private long newOperationId() {
    return nextOperationId.getAndIncrement();
  }

  public LoggingAuditor(final String name,
      final IOStatisticsStore iostatistics) {

    super(name, iostatistics);
    processID = PROCESS_ID;
    final CommonAuditContext currentContext = currentContext();
    warningSpan = new WarningSpan("Operation without an audit span",
        currentContext, newOperationId(), null, null);
    // add the principal
    String p;
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      p = ugi.getUserName();
      attributes.put(PRINCIPAL, p);
    } catch (IOException ignored) {
      p = "";
    }
    principal = p;
  }

  /**
   * Service init, look for jobID and attach as an attribute in log entries.
   * @param conf configuration
   * @throws Exception failure
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    rejectOutOfSpan = conf.getBoolean(
        AuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS, false);
    // attach the job ID if there is one in the configuration used
    // to create this file.
    String jobID = extractJobID(conf);
    if (jobID != null) {
      attributes.put(AuditConstants.JOB_ID, jobID);
    }
  }

  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2) {
    getIOStatistics().incrementCounter(
        Statistic.AUDIT_SPAN_START.getSymbol());
    final LoggingAuditSpan span = new LoggingAuditSpan(name,
        prepareActiveContext(), newOperationId(), path1, path2);
    span.start();
    return span;
  }

  /**
   * Get/Prepare the active context for a span.
   * Includes patching in the principal if unset.
   * @return the common audit context.
   */
  private CommonAuditContext prepareActiveContext() {
    final CommonAuditContext currentContext = currentContext();
    // put principal in current context if unset.
    if (!currentContext.containsKey(PRINCIPAL)) {
      currentContext.put(PRINCIPAL, principal);
    }

    return currentContext;
  }

  @Override
  public AuditSpan getUnbondedSpan() {
    return warningSpan;
  }

  public String getProcessID() {
    return processID;
  }

  /**
   * Span which logs.
   */
  private class LoggingAuditSpan extends AbstractAuditSpanImpl {

    private final HttpReferrerAuditEntry entry;

    private final String operationName;

    private final String description;

    private final CommonAuditContext context;

    private final String id;

    private LoggingAuditSpan(
        final String name,
        final CommonAuditContext context,
        final long operationId,
        final String path1, final String path2) {
      this.operationName = name;
      this.context = context;
      this.id = String.format("%s-%08d", getProcessID(), operationId);
      entry = HttpReferrerAuditEntry.builder()
          .withContext(processID)
          .withOperationId(String.format("%08x", operationId))
          .withOperationName(name)
          .withPath1(path1)
          .withPath2(path2)
          .withAttributes(attributes)
          .build();
      this.description = entry.getReferrerHeader();
    }

    public void start() {
      LOG.trace("{} Start {}", getId(), getDescription());
    }

    protected String getOperationName() {
      return operationName;
    }

    protected String getDescription() {
      return description;
    }

    protected String getId() {
      return id;
    }

    @Override
    public AuditSpan activate() {
      LOG.trace("{} Activate {}", id, description);
      return this;
    }

    @Override
    public void deactivate() {
      LOG.trace("{} Deactivate {}", id, description);
    }

    @Override
    public <T extends AmazonWebServiceRequest> T beforeExecution(
        final T request) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} Executing {} with {}; {}", id,
            getOperationName(),
            analyzer.analyze(request),
            getDescription());
      }
      // add the referrer header
      request.putCustomRequestHeader(HEADER_REFERRER, entry.getReferrerHeader());
      return request;
    }

    @Override
    public void afterResponse(final Request<?> request,
        final Response<?> response) {
      final Object awsResponse = response.getAwsResponse();
    }

    @Override
    public void afterError(final Request<?> request,
        final Response<?> response,
        final Exception exception) {

    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "LoggingAuditSpan{");
      sb.append(", id='").append(id).append('\'');
      sb.append("description='").append(description).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Span which logs at WARN; used to highlight spans
   * without a containing span.
   */
  private final class WarningSpan extends LoggingAuditSpan {

    private WarningSpan(
        final String name,
        final CommonAuditContext context,
        final long operationId,
        final String path1, final String path2) {
      super(name, context, operationId, path1, path2);
    }

    @Override
    public void start() {
      LOG.warn("{} Start {}", getId(), getDescription());
    }

    @Override
    public AuditSpan activate() {
      LOG.warn("{} Activate {}", getId(), getDescription());
      return this;
    }

    @Override
    public boolean isValidSpan() {
      return false;
    }

    @Override
    public <T extends AmazonWebServiceRequest> T requestCreated(
        final T request) {
      String error = "Creating a request outside an audit span "
          + analyzer.analyze(request);
      LOG.info(error);
      if (LOG.isDebugEnabled()) {
        LOG.debug(error, new AuditFailureException("unaudited"));
      }
      return request;
    }

    /**
     * Handle requests made without a real context by logging and
     * increment the failure count.
     * Some requests (e.g. copy part) are not expected in spans due
     * to how they are executed; these do not trigger failures.
     * @param request request
     * @param <T> type of request
     * @return an updated request.
     * @throws AuditFailureException if failure is enabled.
     */
    @Override
    public <T extends AmazonWebServiceRequest> T beforeExecution(
        final T request) {

      getIOStatistics().incrementCounter(
          Statistic.AUDIT_SPAN_START.getSymbol() + SUFFIX_FAILURES);
      String error = "Executing a request outside an audit span "
          + analyzer.analyze(request);
      LOG.warn("{} {}",
          getId(), error);
      final String unaudited = getId() + " "
          + UNAUDITED_OPERATION + " " + error;
      if (isRequestNotAlwaysInSpan(request)) {
        // can get by auditing during a copy, so don't overreact
        LOG.debug(unaudited);
      } else {
        final RuntimeException ex = new AuditFailureException(unaudited);
        LOG.info(unaudited, ex);
        if (rejectOutOfSpan) {
          throw ex;
        }
      }
      // now hand off to the superclass for its normal preparation
      return super.beforeExecution(request);
    }
  }
}
