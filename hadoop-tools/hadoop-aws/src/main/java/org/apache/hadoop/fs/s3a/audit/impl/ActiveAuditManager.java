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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.SdkBaseException;
import com.amazonaws.handlers.RequestHandler2;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer;
import org.apache.hadoop.fs.s3a.audit.AuditConstants;
import org.apache.hadoop.fs.s3a.audit.AuditFailureException;
import org.apache.hadoop.fs.s3a.audit.AuditIntegration;
import org.apache.hadoop.fs.s3a.audit.AuditManager;
import org.apache.hadoop.fs.s3a.audit.AuditSpan;
import org.apache.hadoop.fs.s3a.audit.AuditSpanCallbacks;
import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.CompositeService;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.audit.AuditInternalConstants.AUDIT_SPAN_HANDLER_CONTEXT;

/**
 * Thread management for the active audit.
 * This should be created by whatever wants
 * to have active audit span tracking.
 * Creates and starts the actual
 * {@link OperationAuditor} for auditing.
 * It stores the thread specific span and returns a wrapping
 * span.
 * When the wrapper is closed/deactivated it
 * will deactivate the wrapped span and then
 * switch the active span to the unbounded span.
 */
@InterfaceAudience.Private
public final class ActiveAuditManager
    extends CompositeService
    implements AuditSpanCallbacks, AuditManager {

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ActiveAuditManager.class);

  /**
   * Audit service.
   */
  private OperationAuditor auditService;

  /**
   * Some basic analysis for the logs.
   */
  private final AWSRequestAnalyzer analyzer = new AWSRequestAnalyzer();

  /**
   * This is the span returned to after a wrapper is closed or
   * the span is reset to the unbonded span..
   */
  private WrappingAuditSpan unboundedSpan;

  /**
   * Thread local span. This defaults to being
   * the unbonded span.
   */
  private final ThreadLocal<WrappingAuditSpan> activeSpan =
      ThreadLocal.withInitial(() -> getUnboundedSpan());

  /**
   * Destination for recording statistics, especially duration/count of
   * operations.
   */
  private final IOStatisticsStore iostatistics;

  /**
   * Instantiate.
   * @param iostatistics statistics target
   */
  public ActiveAuditManager(final IOStatisticsStore iostatistics) {
    super("ActiveAuditManager");
    this.iostatistics = iostatistics;
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    // create and register the service so it follows the same lifecycle
    auditService = AuditIntegration.createAuditor(
        getConfig(),
        AuditConstants.AUDIT_SERVICE_CLASSNAME,
        iostatistics);
    addService(auditService);
    LOG.debug("Audit manager initialized with audit service {}", auditService);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    setUnboundedSpan(new WrappingAuditSpan(
        auditService.getUnbondedSpan(), false));
    LOG.debug("Started audit service {}", auditService);
  }

  /**
   * Get the unbounded span. Until this manager
   * is fully initialized it will return the no-op
   * span.
   * @return the unbounded span.
   */
  private WrappingAuditSpan getUnboundedSpan() {
    return unboundedSpan;
  }

  public void setUnboundedSpan(final WrappingAuditSpan unboundedSpan) {
    this.unboundedSpan = unboundedSpan;
  }

  @Override
  public AuditSpan getActiveThreadSpan() {
    return activeSpan.get();
  }

  /**
   * Set a specific span as the active span.
   * @param span span to use.
   * @return the wrapping span.
   */
  private AuditSpan setActiveThreadSpan(AuditSpan span) {
    return switchToActiveSpan(new WrappingAuditSpan(span, true));
  }

  /**
   * Switch to a given span. If it is null, use the
   * unbounded span.
   * @param span to switch to.
   * @return the span switched to
   */
  private WrappingAuditSpan switchToActiveSpan(WrappingAuditSpan span) {
    if (span != null && span.isValidSpan()) {
      activeSpan.set(span);
    } else {
      activeSpan.set(unboundedSpan);
    }
    return activeSpan.get();
  }

  /**
   * Start an operation; as well as invoking the audit
   * service to do this, sets the operation as the
   * active operation for this thread.
   * @param name operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a wrapped audit span
   * @throws IOException failure
   */
  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    // must be started
    Preconditions.checkState(isInState(STATE.STARTED),
        "Audit Manager %s is in wrong state: %s",
        this, getServiceState());
    return setActiveThreadSpan(auditService.createSpan(
        name, path1, path2));
  }

  /**
   * Return a request handler for the AWS SDK which
   * relays to this class.
   * @return a request handler.
   */
  @Override
  public List<RequestHandler2> createRequestHandlers() {

    // wire up the AWS SDK To call back into this class when
    // preparing to make S3 calls.
    List<RequestHandler2> requestHandlers = new ArrayList<>();
    requestHandlers.add(new SdkRequestHandler());
    return requestHandlers;
  }

  /**
   * Attach a reference to the active thread span, then
   * invoke the same callback on that active thread.
   */
  @Override
  public <T extends AmazonWebServiceRequest> T requestCreated(
      final T request) {
    AuditSpan span = getActiveThreadSpan();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Created Request {} in span {}",
          analyzer.analyze(request), span);
    }
    request.addHandlerContext(AUDIT_SPAN_HANDLER_CONTEXT, span);
    return span.requestCreated(request);
  }

  @Override
  public <T extends AmazonWebServiceRequest> T beforeExecution(
      final T request) {
    // identify the span
    AuditSpanCallbacks span;
    span = request.getHandlerContext(AUDIT_SPAN_HANDLER_CONTEXT);
    if (span == null) {
      LOG.warn("no span attached to request {}", analyzer.analyze(request));
      span = getActiveThreadSpan();
    }
    return span.beforeExecution(request);
  }

  /**
   * Forward to active span.
   * @param request request
   * @param response response.
   */
  @Override
  public void afterResponse(final Request<?> request,
      final Response<?> response)
      throws AuditFailureException, SdkBaseException {

    getActiveThreadSpan().afterResponse(request, response);
  }

  /**
   * Forward to active span.
   * @param request request
   * @param response response.
   * @param exception exception raised.
   */
  @Override
  public void afterError(final Request<?> request,
      final Response<?> response,
      final Exception exception)
      throws AuditFailureException, SdkBaseException {

    getActiveThreadSpan().afterError(request, response, exception);
  }

  /**
   * Callbacks from the AWS SDK; all forward to the ActiveAuditManager.
   */
  private class SdkRequestHandler extends RequestHandler2 {

    @Override
    public AmazonWebServiceRequest beforeExecution(
        final AmazonWebServiceRequest request) {
      return ActiveAuditManager.this.beforeExecution(request);
    }

    @Override
    public void afterResponse(final Request<?> request,
        final Response<?> response) {
      ActiveAuditManager.this.afterResponse(request, response);
    }

    @Override
    public void afterError(final Request<?> request,
        final Response<?> response,
        final Exception e) {
      ActiveAuditManager.this.afterError(request, response, e);
    }
  }


  /**
   * Wraps the plugged in spans with management of the active thread
   * span, including switching to the unbounded span when a valid
   * span is deactivated.
   * Package private for testing.
   */
  final class WrappingAuditSpan extends AbstractAuditSpanImpl {

    /**
     * Inner span.
     */
    private final AuditSpan span;

    /**
     * Is this span considered valid?
     */
    private final boolean isValid;

    /**
     * Create, wrapped.
     * @param span inner span.
     * @param isValid is the span valid
     */
    private WrappingAuditSpan(
        final AuditSpan span, final boolean isValid) {
      this.span = requireNonNull(span);
      this.isValid = isValid;
    }

    /**
     * Is the span active?
     * @return true if this span is the active one for the current thread.
     */
    private boolean isActive() {
      return this == getActiveThreadSpan();
    }

    /**
     * Makes this the thread's active span and activate.
     * If the span was already active: no-op.
     */
    @Override
    public AuditSpan activate() {
      if (!isActive()) {
        switchToActiveSpan(this);
        span.activate();
      }
      return this;
    }

    /**
     * Switch to the unbounded span and then deactivate this span.
     * No-op for invalid spans,
     * so as to prevent the unbounded span from being closed
     * and everything getting very confused.
     */
    @Override
    public void deactivate() {
      // no-op for invalid spans,
      // so as to prevent the unbounded span from being closed
      // and everything getting very confused.
      if (!isValid || !isActive()) {
        return;
      }
      // deactivate the span
      span.deactivate();
      // and go to the unbounded one.
      switchToActiveSpan(getUnboundedSpan());
    }

    /**
     * Forward to the wrapped span.
     * {@inheritDoc}
     */
    @Override
    public <T extends AmazonWebServiceRequest> T requestCreated(
        final T request) {
      return span.requestCreated(request);
    }

    /**
     * This span is valid if the span isn't closed and the inner
     * span is valid.
     * @return true if the span is considered valid.
     */
    @Override
    public boolean isValidSpan() {
      return isValid && span.isValidSpan();
    }

    /**
     * Forward to the inner span.
     * @param request request
     * @param <T> type of request
     * @return an updated request.
     */
    @Override
    public <T extends AmazonWebServiceRequest> T beforeExecution(
        final T request) {
      return span.beforeExecution(request);
    }

    /**
     * Forward to the inner span.
     * @param request request
     * @param response response.
     */
    @Override
    public void afterResponse(final Request<?> request,
        final Response<?> response) {
      span.afterResponse(request, response);
    }

    /**
     * Forward to the inner span.
     * @param request request
     * @param response response.
     * @param exception exception raised.
     */
    @Override
    public void afterError(final Request<?> request,
        final Response<?> response,
        final Exception exception) {
      span.afterError(request, response, exception);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "WrappingAuditSpan{");
      sb.append("span=").append(span);
      sb.append(", valid=").append(isValidSpan());
      sb.append('}');
      return sb.toString();
    }
  }

}
