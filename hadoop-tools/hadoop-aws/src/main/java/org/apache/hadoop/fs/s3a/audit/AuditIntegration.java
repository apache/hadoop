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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.ActiveAuditManager;
import org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditManager;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor;
import org.apache.hadoop.fs.s3a.audit.impl.NoopSpan;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

import static java.util.Objects.requireNonNull;

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
   * Given a callable, return a new callable which
   * activates and deactivates the span around the inner invocation.
   * @param auditSpan audit span
   * @param operation operation
   * @param <T> type of result
   * @return a new invocation.
   */
  public static <T> CallableRaisingIOE<T> withinSpan(
      AuditSpan auditSpan,
      CallableRaisingIOE<T> operation) {
    return () -> {
      try (AuditSpan span = auditSpan.activate()) {
        return operation.apply();
      }
    };
  }

  /**
   * Given a invocation, return a new invocation which
   * activates and deactivates the span around the inner invocation.
   * @param auditSpan audit span
   * @param operation operation
   * @return a new invocation.
   */
  public static InvocationRaisingIOE withinSpan(
      AuditSpan auditSpan,
      InvocationRaisingIOE operation) {
    return () -> {
      try (AuditSpan span = auditSpan.activate()) {
        operation.apply();
      }
    };
  }

  /**
   * Create and start an audit manager.
   * @param conf configuration
   * @param iostatistics IOStatistics source.
   * @return audit manager.
   */
  public static AuditManager createAuditManager(
      Configuration conf,
      IOStatisticsStore iostatistics) {
    ActiveAuditManager auditManager = new ActiveAuditManager(
        requireNonNull(iostatistics));
    auditManager.init(conf);
    auditManager.start();
    return auditManager;
  }

  /**
   * Return a stub audit manager.
   * @return an audit manager.
   */
  public static AuditManager stubAuditManager() {
    return new NoopAuditManager();
  }

  /**
   * Create, and initialize an audit service.
   * The service start operation is not called: that is left to
   * the caller.
   * @param conf configuration to read the key from and to use to init
   * the service.
   * @param key key containing the classname
   * @param store IOStatistics store.
   * @return instantiated class.
   * @throws IOException failure to initialise.
   */
  public static OperationAuditor createAuditor(
      Configuration conf,
      String key,
      IOStatisticsStore store) throws IOException {
    try {
      final Class<? extends OperationAuditor> auditClassname
          = conf.getClass(
          key,
          LoggingAuditor.class,
          OperationAuditor.class);
      LOG.debug("Auditor class is {}", auditClassname);
      final Constructor<? extends OperationAuditor> constructor
          = auditClassname.getConstructor(String.class,
          IOStatisticsStore.class);
      final OperationAuditor instance = constructor.newInstance(
          auditClassname.getCanonicalName(), store);
      instance.init(conf);
      return instance;
    } catch (NoSuchMethodException | InstantiationException
        | RuntimeException
        | IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Failed to instantiate class "
          + "defined in " + key,
          e);
    }
  }

  /**
   * Create, init and start a no-op auditor instance.
   * @param conf configuration.
   * @return a started instance.
   */
  public static OperationAuditor noopAuditor(Configuration conf) {
    return NoopAuditor.newInstance(conf);
  }

  /**
   * Create a no-op span.
   * @param name name
   * @param path1 path
   * @param path2 path 2
   * @return a span.
   */
  public static final AuditSpan noopSpan(final String name,
      final String path1,
      final String path2) {
    return new NoopSpan(name, path1, path2);
  }

  /**
   * Reusable no-op span instance.
   */
  public static AuditSpan NOOP_SPAN = NoopSpan.INSTANCE;
}
