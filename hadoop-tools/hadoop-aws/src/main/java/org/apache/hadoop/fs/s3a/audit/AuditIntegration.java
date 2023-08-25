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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.AccessDeniedException;

import com.amazonaws.HandlerContextAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.api.UnsupportedRequestException;
import org.apache.hadoop.fs.s3a.audit.impl.ActiveAuditManagerS3A;
import org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditManagerS3A;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.audit.impl.S3AInternalAuditConstants.AUDIT_SPAN_HANDLER_CONTEXT;

/**
 * Support for integrating auditing within the S3A code.
 */
public final class AuditIntegration {

  /**
   * Logging.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(AuditIntegration.class);

  private AuditIntegration() {
  }

  /**
   * Create and start an audit manager.
   * @param conf configuration
   * @param iostatistics IOStatistics source.
   * @return audit manager.
   */
  public static AuditManagerS3A createAndStartAuditManager(
      Configuration conf,
      IOStatisticsStore iostatistics) {
    AuditManagerS3A auditManager;
    if (conf.getBoolean(AUDIT_ENABLED, AUDIT_ENABLED_DEFAULT)) {
      auditManager = new ActiveAuditManagerS3A(
          requireNonNull(iostatistics));
    } else {
      LOG.debug("auditing is disabled");
      auditManager = stubAuditManager();
    }
    auditManager.init(conf);
    auditManager.start();
    LOG.debug("Started Audit Manager {}", auditManager);
    return auditManager;
  }

  /**
   * Return a stub audit manager.
   * @return an audit manager.
   */
  public static AuditManagerS3A stubAuditManager() {
    return new NoopAuditManagerS3A();
  }

  /**
   * Create and initialize an audit service.
   * The service start operation is not called: that is left to
   * the caller.
   * @param conf configuration to read the key from and to use to init
   * the service.
   * @param key key containing the classname
   * @param options options to initialize with.
   * @return instantiated class.
   * @throws IOException failure to initialise.
   */
  public static OperationAuditor createAndInitAuditor(
      Configuration conf,
      String key,
      OperationAuditorOptions options) throws IOException {
    final Class<? extends OperationAuditor> auditClassname
        = conf.getClass(
        key,
        LoggingAuditor.class,
        OperationAuditor.class);
    try {
      LOG.debug("Auditor class is {}", auditClassname);
      final Constructor<? extends OperationAuditor> constructor
          = auditClassname.getConstructor();
      final OperationAuditor instance = constructor.newInstance();
      instance.init(options);
      return instance;
    } catch (NoSuchMethodException | InstantiationException
        | RuntimeException
        | IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Failed to instantiate class "
          + auditClassname
          + " defined in " + key
          + ": " + e,
          e);
    }
  }

  /**
   * Get the span from a handler context.
   * @param request request
   * @param <T> type of request.
   * @return the span callbacks or null
   */
  public static <T extends HandlerContextAware> AWSAuditEventCallbacks
      retrieveAttachedSpan(final T request) {
    return request.getHandlerContext(AUDIT_SPAN_HANDLER_CONTEXT);
  }

  /**
   * Attach a span to a handler context.
   * @param request request
   * @param span span to attach
   * @param <T> type of request.
   */
  public static <T extends HandlerContextAware> void attachSpanToRequest(
      final T request, final AWSAuditEventCallbacks span) {
    request.addHandlerContext(AUDIT_SPAN_HANDLER_CONTEXT, span);
  }

  /**
   * Translate an audit exception.
   * @param path path of operation.
   * @param exception exception
   * @return the IOE to raise.
   */
  public static IOException translateAuditException(String path,
      AuditFailureException exception) {
    if (exception instanceof AuditOperationRejectedException) {
      // special handling of this subclass
      return new UnsupportedRequestException(path,
          exception.getMessage(), exception);
    }
    return (AccessDeniedException)new AccessDeniedException(path, null,
        exception.toString()).initCause(exception);
  }
}
