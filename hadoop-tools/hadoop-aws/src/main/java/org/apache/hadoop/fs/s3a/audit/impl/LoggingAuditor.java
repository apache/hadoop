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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.audit.AuditConstants;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer;
import org.apache.hadoop.fs.s3a.audit.AuditFailureException;
import org.apache.hadoop.fs.s3a.audit.AuditOperationRejectedException;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.audit.AuditConstants.DELETE_KEYS_SIZE;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_FILESYSTEM_ID;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PRINCIPAL;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD0;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_TIMESTAMP;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentThreadID;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MULTIPART_UPLOAD_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_UPLOADS_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer.isRequestMultipartIO;
import static org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer.isRequestNotAlwaysInSpan;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.OUTSIDE_SPAN;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REFERRER_HEADER_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REFERRER_HEADER_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REFERRER_HEADER_FILTER;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.UNAUDITED_OPERATION;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.extractJobID;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.HEADER_REFERRER;

/**
 * The LoggingAuditor logs operations at DEBUG (in SDK Request) and
 * in span lifecycle and S3 request class construction at TRACE.
 * The context information is added as the HTTP referrer.
 */
@InterfaceAudience.Private
public class LoggingAuditor
    extends AbstractOperationAuditor {

  /**
   * This is where the context gets logged to.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(LoggingAuditor.class);


  /**
   * Some basic analysis for the logs.
   */
  private final AWSRequestAnalyzer analyzer = new AWSRequestAnalyzer();

  /**
   * Default span to use when there is no other.
   */
  private AuditSpanS3A warningSpan;

  /**
   * Should out of scope ops be rejected?
   */
  private boolean rejectOutOfSpan;

  /**
   * Map of attributes which will be added to all operations.
   */
  private final Map<String, String> attributes = new HashMap<>();

  /**
   * Should the referrer header be added?
   */
  private boolean headerEnabled;

  /**
   * This is the header sent by the last S3 operation through
   * this auditor.
   * <p>
   * It is for testing -allows for Integration tests to
   * verify that a header was sent and query what was in it.
   * Initially an empty string.
   */
  private volatile String lastHeader = "";

  /**
   * Attributes to filter.
   */
  private Collection<String> filters;

  /**
   * Does the S3A FS instance being audited have multipart upload enabled?
   * If not: fail if a multipart upload is initiated.
   */
  private boolean isMultipartUploadEnabled;

  /**
   * Log for warning of problems getting the range of GetObjectRequest
   * will only log of a problem once per process instance.
   * This is to avoid logs being flooded with errors.
   */
  private static final LogExactlyOnce WARN_INCORRECT_RANGE =
      new LogExactlyOnce(LOG);

  /**
   * Create the auditor.
   * The UGI current user is used to provide the principal;
   * this will be cached and provided in the referrer header.
   */
  public LoggingAuditor() {

    super("LoggingAuditor ");
    attributes.put(PARAM_FILESYSTEM_ID, getAuditorId());


    // add the principal
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      addAttribute(PARAM_PRINCIPAL, ugi.getUserName());
    } catch (IOException ex) {
      LOG.warn("Auditor unable to determine principal", ex);
    }
  }

  /**
   * Service init, look for jobID and attach as an attribute in log entries.
   * This is where the warning span is created, so the relevant attributes
   * (and filtering options) are applied.
   * @param conf configuration
   * @throws Exception failure
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    rejectOutOfSpan = conf.getBoolean(
        REJECT_OUT_OF_SPAN_OPERATIONS, false);
    // attach the job ID if there is one in the configuration used
    // to create this file.
    String jobID = extractJobID(conf);
    if (jobID != null) {
      addAttribute(AuditConstants.PARAM_JOB_ID, jobID);
    }
    headerEnabled = getConfig().getBoolean(REFERRER_HEADER_ENABLED,
        REFERRER_HEADER_ENABLED_DEFAULT);
    filters = conf.getTrimmedStringCollection(REFERRER_HEADER_FILTER);
    final CommonAuditContext currentContext = currentAuditContext();
    warningSpan = new WarningSpan(OUTSIDE_SPAN,
        currentContext, createSpanID(), null, null);
    isMultipartUploadEnabled = conf.getBoolean(MULTIPART_UPLOADS_ENABLED,
              DEFAULT_MULTIPART_UPLOAD_ENABLED);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "LoggingAuditor{");
    sb.append("ID='").append(getAuditorId()).append('\'');
    sb.append(", headerEnabled=").append(headerEnabled);
    sb.append(", rejectOutOfSpan=").append(rejectOutOfSpan);
    sb.append(", isMultipartUploadEnabled=").append(isMultipartUploadEnabled);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public AuditSpanS3A createSpan(final String operation,
      @Nullable final String path1,
      @Nullable final String path2) {
    LoggingAuditSpan span = new LoggingAuditSpan(
        createSpanID(),
        operation,
        prepareActiveContext(),
        path1,
        path2);
    span.start();
    return span;
  }

  /**
   * Get/Prepare the active context for a span.
   * @return the common audit context.
   */
  private CommonAuditContext prepareActiveContext() {
    return currentAuditContext();
  }

  /**
   * Add an attribute.
   * @param key key
   * @param value value
   */
  public final void addAttribute(String key, String value) {
    attributes.put(key, value);
  }

  @Override
  public AuditSpanS3A getUnbondedSpan() {
    return warningSpan;
  }

  /**
   * Get the last header used.
   * @return the last referrer header generated.
   */
  public String getLastHeader() {
    return lastHeader;
  }

  /**
   * Set that last header.
   * @param lastHeader the value for the lastHeader field.
   */
  private void setLastHeader(final String lastHeader) {
    this.lastHeader = lastHeader;
  }

  /**
   * Span which logs at debug and sets the HTTP referrer on
   * invocations.
   * Note: checkstyle complains that this should be final because
   * it is private. This is not true, as it is subclassed in
   * the same file.
   */
  private class LoggingAuditSpan extends AbstractAuditSpanImpl {

    private final HttpReferrerAuditHeader referrer;

    /**
     * Attach Range of data for GetObject Request.
     * @param request given get object request
     */
    private void attachRangeFromRequest(AmazonWebServiceRequest request) {
      if (request instanceof GetObjectRequest) {
        long[] rangeValue = ((GetObjectRequest) request).getRange();
        if (rangeValue == null || rangeValue.length == 0) {
          return;
        }
        if (rangeValue.length != 2) {
          WARN_INCORRECT_RANGE.warn("Expected range to contain 0 or 2 elements."
              + " Got {} elements. Ignoring.", rangeValue.length);
          return;
        }
        String combinedRangeValue = String.format("%d-%d", rangeValue[0], rangeValue[1]);
        referrer.set(AuditConstants.PARAM_RANGE, combinedRangeValue);
      }
    }

    private final String description;

    private LoggingAuditSpan(
        final String spanId,
        final String operationName,
        final CommonAuditContext context,
        final String path1,
        final String path2) {
      super(spanId, operationName);

      this.referrer = HttpReferrerAuditHeader.builder()
          .withContextId(getAuditorId())
          .withSpanId(spanId)
          .withOperationName(operationName)
          .withPath1(path1)
          .withPath2(path2)
          .withAttributes(attributes)
          // thread at the time of creation.
          .withAttribute(PARAM_THREAD0,
              currentThreadID())
          .withAttribute(PARAM_TIMESTAMP, Long.toString(getTimestamp()))
          .withEvaluated(context.getEvaluatedEntries())
          .withFilter(filters)
          .build();

      this.description = referrer.buildHttpReferrer();
    }

    public void start() {
      LOG.trace("{} Start {}", getSpanId(), getDescription());
    }

    /**
     * Get the span description built in the constructor.
     * @return description text.
     */
    protected String getDescription() {
      return description;
    }

    /**
     * Activate: log at TRACE.
     * @return this span.
     */
    @Override
    public AuditSpanS3A activate() {
      LOG.trace("[{}] {} Activate {}",
          currentThreadID(), getSpanId(), getDescription());
      return this;
    }

    /**
     * Log at TRACE.
     */
    @Override
    public void deactivate() {
      LOG.trace("[{}] {} Deactivate {}",
          currentThreadID(), getSpanId(), getDescription());
    }


    /**
     * Pass to the HTTP referrer.
     * {@inheritDoc}
     */
    @Override
    public void set(final String key, final String value) {
      referrer.set(key, value);
    }

    /**
     * Before execution, the logging auditor always builds
     * the referrer header, saves to the outer class
     * (where {@link #getLastHeader()} can retrieve it,
     * and logs at debug.
     * If configured to add the header to the S3 logs, it will
     * be set as the HTTP referrer.
     * @param request request
     * @param <T> type of request.
     * @return the request with any extra headers.
     */
    @Override
    public <T extends AmazonWebServiceRequest> T beforeExecution(
        final T request) {
      // attach range for GetObject requests
      attachRangeFromRequest(request);
      // for delete op, attach the number of files to delete
      attachDeleteKeySizeAttribute(request);
      // build the referrer header
      final String header = referrer.buildHttpReferrer();
      // update the outer class's field.
      setLastHeader(header);
      if (headerEnabled) {
        // add the referrer header
        request.putCustomRequestHeader(HEADER_REFERRER,
            header);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("[{}] {} Executing {} with {}; {}",
            currentThreadID(),
            getSpanId(),
            getOperationName(),
            analyzer.analyze(request),
            header);
      }
      // now see if the request is actually a blocked multipart request
      if (!isMultipartUploadEnabled && isRequestMultipartIO(request)) {
        throw new AuditOperationRejectedException("Multipart IO request "
            + request + " rejected " + header);
      }

      return request;
    }

    /**
     * For delete requests, attach delete key size as a referrer attribute.
     *
     * @param request the request object.
     * @param <T> type of the request.
     */
    private <T extends AmazonWebServiceRequest> void attachDeleteKeySizeAttribute(T request) {
      if (request instanceof DeleteObjectsRequest) {
        int keySize = ((DeleteObjectsRequest) request).getKeys().size();
        this.set(DELETE_KEYS_SIZE, String.valueOf(keySize));
      } else if (request instanceof DeleteObjectRequest) {
        String key = ((DeleteObjectRequest) request).getKey();
        if (key != null && key.length() > 0) {
          this.set(DELETE_KEYS_SIZE, "1");
        }
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "LoggingAuditSpan{");
      sb.append(", id='").append(getSpanId()).append('\'');
      sb.append("description='").append(description).append('\'');
      sb.append('}');
      return sb.toString();
    }

    /**
     * Get the referrer; visible for tests.
     * @return the referrer.
     */
    HttpReferrerAuditHeader getReferrer() {
      return referrer;
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
        final String spanId,
        final String path1, final String path2) {
      super(spanId, name, context, path1, path2);
    }

    @Override
    public void start() {
      LOG.warn("[{}] {} Start {}",
          currentThreadID(), getSpanId(), getDescription());
    }

    @Override
    public AuditSpanS3A activate() {
      LOG.warn("[{}] {} Activate {}",
          currentThreadID(), getSpanId(), getDescription());
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

      String error = "executing a request outside an audit span "
          + analyzer.analyze(request);
      final String unaudited = getSpanId() + " "
          + UNAUDITED_OPERATION + " " + error;
      if (isRequestNotAlwaysInSpan(request)) {
        // can get by auditing during a copy, so don't overreact
        LOG.debug(unaudited);
      } else {
        final RuntimeException ex = new AuditFailureException(unaudited);
        LOG.debug(unaudited, ex);
        if (rejectOutOfSpan) {
          throw ex;
        }
      }
      // now hand off to the superclass for its normal preparation
      return super.beforeExecution(request);
    }
  }
}
