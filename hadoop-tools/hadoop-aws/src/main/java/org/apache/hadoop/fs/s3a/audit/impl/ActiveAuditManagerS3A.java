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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HandlerContextAware;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.SdkBaseException;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerBeforeAttemptContext;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.audit.AWSAuditEventCallbacks;
import org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer;
import org.apache.hadoop.fs.s3a.audit.AuditFailureException;
import org.apache.hadoop.fs.s3a.audit.AuditIntegration;
import org.apache.hadoop.fs.s3a.audit.AuditManagerS3A;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.audit.OperationAuditor;
import org.apache.hadoop.fs.s3a.audit.OperationAuditorOptions;
import org.apache.hadoop.fs.s3a.audit.S3AAuditConstants;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_FAILURE;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_REQUEST_EXECUTION;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.attachSpanToRequest;
import static org.apache.hadoop.fs.s3a.audit.AuditIntegration.retrieveAttachedSpan;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_REQUEST_HANDLERS;

/**
 * Thread management for the active audit.
 * This should be created by whatever wants to have active
 * audit span tracking.
 *
 * It creates and starts the actual
 * {@link OperationAuditor} for auditing.
 * It then stores the thread-local span and returns a wrapping
 * span.
 *
 * When the wrapper is closed/deactivated it
 * will deactivate the wrapped span and then
 * switch the active span to the unbounded span.
 *
 * The inner class {@link AWSAuditEventCallbacks} is returned
 * as a request handler in {@link #createRequestHandlers()};
 * this forwards all requests to the outer {@code ActiveAuditManagerS3A},
 * which then locates the active span and forwards the request.
 * If any such invocation raises an {@link AuditFailureException}
 * then the IOStatistics counter for {@code AUDIT_FAILURE}
 * is incremented.
 *
 */
@InterfaceAudience.Private
public final class ActiveAuditManagerS3A
    extends CompositeService
    implements AuditManagerS3A {

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ActiveAuditManagerS3A.class);

  /**
   * One of logger for warnings about span retrieval.
   */
  public static final LogExactlyOnce WARN_OF_SPAN_TYPE =
      new LogExactlyOnce(LOG);

  public static final String AUDIT_MANAGER_OPERATION = "AuditManagerS3A";

  public static final String NOT_A_WRAPPED_SPAN
      = "Span attached to request is not a wrapped span";

  /**
   * Audit service.
   */
  private OperationAuditor auditor;

  /**
   * Some basic analysis for the logs.
   */
  private final AWSRequestAnalyzer analyzer = new AWSRequestAnalyzer();

  /**
   * This is the span returned to after a wrapper is closed or
   * the span is reset to the unbonded span..
   */
  private WrappingAuditSpan unbondedSpan;

  /**
   * Thread local span. This defaults to being
   * the unbonded span.
   */
  private final ThreadLocal<WrappingAuditSpan> activeSpan =
      ThreadLocal.withInitial(() -> getUnbondedSpan());

  /**
   * Destination for recording statistics, especially duration/count of
   * operations.
   */
  private final IOStatisticsStore ioStatisticsStore;

  /**
   * Instantiate.
   * @param iostatistics statistics target
   */
  public ActiveAuditManagerS3A(final IOStatisticsStore iostatistics) {
    super("ActiveAuditManagerS3A");
    this.ioStatisticsStore = iostatistics;
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    // create and register the service so it follows the same lifecycle
    OperationAuditorOptions options =
        OperationAuditorOptions.builder()
            .withConfiguration(conf)
            .withIoStatisticsStore(ioStatisticsStore);
    auditor = AuditIntegration.createAndInitAuditor(
        getConfig(),
        S3AAuditConstants.AUDIT_SERVICE_CLASSNAME,
        options);
    addService(auditor);
    LOG.debug("Audit manager initialized with audit service {}", auditor);
  }

  /**
   * After starting the auditor, it is queried for its
   * unbonded span, which is then wrapped and stored for
   * use.
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    setUnbondedSpan(new WrappingAuditSpan(
        auditor.getUnbondedSpan(), false));
    LOG.debug("Started audit service {}", auditor);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    sb.append(", auditor=").append(auditor);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public OperationAuditor getAuditor() {
    return auditor;
  }

  /**
   * Get the unbounded span. Until this manager
   * is fully initialized it will return the no-op
   * span.
   * @return the unbounded span.
   */
  private WrappingAuditSpan getUnbondedSpan() {
    return unbondedSpan;
  }

  /**
   * Set the unbonded span.
   * @param unbondedSpan the new unbonded span
   */
  private void setUnbondedSpan(final WrappingAuditSpan unbondedSpan) {
    this.unbondedSpan = unbondedSpan;
  }

  /**
   * Return the active wrapped span.
   * @return a span.
   */
  @Override
  public AuditSpanS3A getActiveAuditSpan() {
    return activeSpan();
  }

  /**
   * Get the active span.
   * This is the wrapped span, not the inner one, and it is
   * of that type.
   * @return the active WrappingAuditSpan
   */
  private WrappingAuditSpan activeSpan() {
    return activeSpan.get();
  }

  /**
   * Set a specific span as the active span.
   * This will wrap it.
   * @param span span to use.
   * @return the wrapped span.
   */
  private AuditSpanS3A setActiveThreadSpan(AuditSpanS3A span) {
    return switchToActiveSpan(
        new WrappingAuditSpan(span, span.isValidSpan()));
  }

  /**
   * Switch to a given span. If it is null, use the
   * unbounded span.
   * @param span to switch to; may be null
   * @return the span switched to
   */
  private WrappingAuditSpan switchToActiveSpan(WrappingAuditSpan span) {
    if (span != null && span.isValidSpan()) {
      activeSpan.set(span);
    } else {
      activeSpan.set(unbondedSpan);
    }
    return activeSpan();
  }

  /**
   * The Span ID in the audit manager is the ID of the auditor,
   * which can be used in the filesystem toString() method
   * to assist in correlating client logs with S3 logs.
   * It is returned here as part of the implementation of
   * {@link AWSAuditEventCallbacks}.
   * @return the unique ID of the FS.
   */
  @Override
  public String getSpanId() {
    return auditor != null
        ? auditor.getAuditorId()
        : "(auditor not yet created)";
  }

  @Override
  public String getOperationName() {
    return AUDIT_MANAGER_OPERATION;
  }

  /**
   * Start an operation; as well as invoking the audit
   * service to do this, sets the operation as the
   * active operation for this thread.
   * @param operation operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a wrapped audit span
   * @throws IOException failure
   */
  @Override
  public AuditSpanS3A createSpan(final String operation,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    // must be started
    Preconditions.checkState(isInState(STATE.STARTED),
        "Audit Manager %s is in wrong state: %s",
        this, getServiceState());
    ioStatisticsStore.incrementCounter(
        Statistic.AUDIT_SPAN_CREATION.getSymbol());
    return setActiveThreadSpan(auditor.createSpan(
        operation, path1, path2));
  }

  /**
   * Return a request handler for the AWS SDK which
   * relays to this class.
   * @return a request handler.
   */
  @Override
  public List<RequestHandler2> createRequestHandlers()
      throws IOException {

    // wire up the AWS SDK To call back into this class when
    // preparing to make S3 calls.
    List<RequestHandler2> requestHandlers = new ArrayList<>();
    requestHandlers.add(new SdkRequestHandler());
    // now look for any more handlers
    final Class<?>[] handlers = getConfig().getClasses(AUDIT_REQUEST_HANDLERS);
    if (handlers != null) {
      for (Class<?> handler : handlers) {
        try {
          Constructor<?> ctor = handler.getConstructor();
          requestHandlers.add((RequestHandler2)ctor.newInstance());
        } catch (ExceptionInInitializerError e) {
          throw FutureIO.unwrapInnerException(e);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    return requestHandlers;
  }

  @Override
  public TransferStateChangeListener createStateChangeListener() {
    final WrappingAuditSpan span = activeSpan();
    return new TransferStateChangeListener() {
      @Override
      public void transferStateChanged(final Transfer transfer,
          final Transfer.TransferState state) {
        switchToActiveSpan(span);
      }
    };
  }

  @Override
  public boolean checkAccess(final Path path,
      final S3AFileStatus status,
      final FsAction mode)
      throws IOException {
    return auditor.checkAccess(path, status, mode);
  }

  /**
   * Attach a reference to the active thread span, then
   * invoke the same callback on that active thread.
   */
  @Override
  public <T extends AmazonWebServiceRequest> T requestCreated(
      final T request) {
    AuditSpanS3A span = getActiveAuditSpan();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Created Request {} in span {}",
          analyzer.analyze(request), span);
    }
    attachSpanToRequest(request, span);
    try {
      return span.requestCreated(request);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  /**
   * Forward to the active span.
   * All invocations increment the statistics counter for
   * {@link Statistic#AUDIT_REQUEST_EXECUTION};
   * failures will also increment
   * {@link Statistic#AUDIT_FAILURE};
   * {@inheritDoc}
   */
  @Override
  public <T extends AmazonWebServiceRequest> T beforeExecution(
      final T request) {
    ioStatisticsStore.incrementCounter(AUDIT_REQUEST_EXECUTION.getSymbol());

    // identify the span and invoke the callback
    try {
      return extractAndActivateSpanFromRequest(request)
          .beforeExecution(request);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
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
    try {
      extractAndActivateSpanFromRequest(request)
          .afterResponse(request, response);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  /**
   * Get the active span from the handler context,
   * falling back to the active thread span if there
   * is nothing in the context.
   * Provided the span is a wrapped span, the
   * @param request request
   * @param <T> type of request.
   * @return the callbacks
   */
  private <T extends HandlerContextAware> AWSAuditEventCallbacks
      extractAndActivateSpanFromRequest(final T request) {
    AWSAuditEventCallbacks span;
    span = retrieveAttachedSpan(request);
    if (span == null) {
      // no span is attached. Not unusual for the copy operations,
      // or for calls to GetBucketLocation made by the AWS client
      LOG.debug("No audit span attached to request {}",
          request);
      // fall back to the active thread span.
      // this will be the unbonded span if the thread is unbonded.
      span = getActiveAuditSpan();
    } else {
      if (span instanceof WrappingAuditSpan) {
        switchToActiveSpan((WrappingAuditSpan) span);
      } else {
        // warn/log and continue without switching.
        WARN_OF_SPAN_TYPE.warn(NOT_A_WRAPPED_SPAN + ": {}", span);
        LOG.debug(NOT_A_WRAPPED_SPAN + ": {}", span);
      }
    }
    return span;
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
    try {
      extractAndActivateSpanFromRequest(request)
          .afterError(request, response, exception);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  @Override
  public AmazonWebServiceRequest beforeMarshalling(
      final AmazonWebServiceRequest request) {
    try {
      return extractAndActivateSpanFromRequest(request)
          .beforeMarshalling(request);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  @Override
  public void beforeRequest(final Request<?> request) {
    try {
      extractAndActivateSpanFromRequest(request)
          .beforeRequest(request);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  @Override
  public void beforeAttempt(final HandlerBeforeAttemptContext context) {
    try {
      extractAndActivateSpanFromRequest(context.getRequest())
          .beforeAttempt(context);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  @Override
  public void afterAttempt(final HandlerAfterAttemptContext context) {
    try {
      extractAndActivateSpanFromRequest(context.getRequest())
          .afterAttempt(context);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
  }

  @Override
  public HttpResponse beforeUnmarshalling(final Request<?> request,
      final HttpResponse httpResponse) {
    try {
      extractAndActivateSpanFromRequest(request.getOriginalRequest())
          .beforeUnmarshalling(request, httpResponse);
    } catch (AuditFailureException e) {
      ioStatisticsStore.incrementCounter(AUDIT_FAILURE.getSymbol());
      throw e;
    }
    return httpResponse;
  }

  /**
   * Callbacks from the AWS SDK; all forward to the ActiveAuditManagerS3A.
   * We need a separate class because the SDK requires the handler list
   * to be list of {@code RequestHandler2} instances.
   */
  private class SdkRequestHandler extends RequestHandler2 {

    @Override
    public AmazonWebServiceRequest beforeExecution(
        final AmazonWebServiceRequest request) {
      return ActiveAuditManagerS3A.this.beforeExecution(request);
    }

    @Override
    public void afterResponse(final Request<?> request,
        final Response<?> response) {
      ActiveAuditManagerS3A.this.afterResponse(request, response);
    }

    @Override
    public void afterError(final Request<?> request,
        final Response<?> response,
        final Exception e) {
      ActiveAuditManagerS3A.this.afterError(request, response, e);
    }

    @Override
    public AmazonWebServiceRequest beforeMarshalling(
        final AmazonWebServiceRequest request) {
      return ActiveAuditManagerS3A.this.beforeMarshalling(request);
    }

    @Override
    public void beforeRequest(final Request<?> request) {
      ActiveAuditManagerS3A.this.beforeRequest(request);
    }

    @Override
    public void beforeAttempt(
        final HandlerBeforeAttemptContext context) {
      ActiveAuditManagerS3A.this.beforeAttempt(context);
    }

    @Override
    public HttpResponse beforeUnmarshalling(
        final Request<?> request,
        final HttpResponse httpResponse) {
      return ActiveAuditManagerS3A.this.beforeUnmarshalling(request,
          httpResponse);
    }

    @Override
    public void afterAttempt(
        final HandlerAfterAttemptContext context) {
      ActiveAuditManagerS3A.this.afterAttempt(context);
    }
  }

  /**
   * Wraps the plugged in spans with management of the active thread
   * span, including switching to the unbounded span when a valid
   * span is deactivated.
   * Package-private for testing.
   */
  private final class WrappingAuditSpan extends AbstractAuditSpanImpl {

    /**
     * Inner span.
     */
    private final AuditSpanS3A span;

    /**
     * Is this span considered valid?
     */
    private final boolean isValid;

    /**
     * Create, wrapped.
     * The spanID, name, timestamp etc copied from the span being wrapped.
     * Why not the isValid state? We want to set our unbonded span without
     * relying on the auditor doing the right thing.
     * @param span inner span.
     * @param isValid is the span valid
     */
    private WrappingAuditSpan(
        final AuditSpanS3A span, final boolean isValid) {
      super(span.getSpanId(), span.getTimestamp(), span.getOperationName());
      this.span = requireNonNull(span);
      this.isValid = isValid;
    }

    /**
     * Is the span active?
     * @return true if this span is the active one for the current thread.
     */
    private boolean isActive() {
      return this == getActiveAuditSpan();
    }

    /**
     * Makes this the thread's active span and activate.
     * If the span was already active: no-op.
     */
    @Override
    public AuditSpanS3A activate() {
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
      switchToActiveSpan(getUnbondedSpan());
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
     * {@inheritDoc}
     */
    @Override
    public void set(final String key, final String value) {
      span.set(key, value);
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

    /**
     * Forward to the inner span.
     * @param request request
     * @return request to marshall
     */
    @Override
    public AmazonWebServiceRequest beforeMarshalling(
        final AmazonWebServiceRequest request) {
      return span.beforeMarshalling(request);
    }

    /**
     * Forward to the inner span.
     * @param request request
     */
    @Override
    public void beforeRequest(final Request<?> request) {
      span.beforeRequest(request);
    }

    /**
     * Forward to the inner span.
     * @param context full context, including the request.
     */
    @Override
    public void beforeAttempt(
        final HandlerBeforeAttemptContext context) {
      span.beforeAttempt(context);
    }

    /**
     * Forward to the inner span.
     *
     * @param context full context, including the request.
     */
    @Override
    public void afterAttempt(
        final HandlerAfterAttemptContext context) {
      span.afterAttempt(context);
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
