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

import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.AuditManager;
import org.apache.hadoop.fs.s3a.audit.AuditSpan;
import org.apache.hadoop.service.AbstractService;

/**
 * Simple No-op audit manager for use before a real
 * audit chain is set up, and for testing.
 * Audit spans always have a unique ID and the activation/deactivation
 * operations on them will update this audit manager's active span.
 * It does have the service lifecycle, so do
 * create a unique instance whenever used.
 */
@InterfaceAudience.Private
public class NoopAuditManager extends AbstractService
    implements AuditManager, NoopSpan.SpanActivationCallbacks {

  private static final NoopAuditor NOOP_AUDITOR =
      NoopAuditor.newInstance(new Configuration(), null);

  /**
   * The inner auditor.
   */
  private final NoopAuditor auditor;

  /**
   * Thread local span. This defaults to being
   * the unbonded span.
   */
  private final ThreadLocal<AuditSpan> activeSpan =
      ThreadLocal.withInitial(this::getUnbondedSpan);

  /**
   * Constructor.
   */
  public NoopAuditManager() {
    super("NoopAuditManager");
    auditor = NoopAuditor.newInstance(new Configuration(), this);
  }

  /**
   * Unbonded span to use after deactivation.
   */
  private AuditSpan getUnbondedSpan() {
    return auditor.getUnbondedSpan();
  }

  @Override
  public AuditSpan getActiveThreadSpan() {
    return NoopSpan.INSTANCE;
  }

  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    return createNewSpan(name, path1, path2);
  }

  @Override
  public List<RequestHandler2> createRequestHandlers() {
    return new ArrayList<>();
  }

  @Override
  public TransferStateChangeListener createStateChangeListener() {
    return new TransferStateChangeListener() {
      public void transferStateChanged(final Transfer transfer,
          final Transfer.TransferState state) {
      }
    };
  }

  @Override
  public void activate(final AuditSpan span) {
    activeSpan.set(span);
  }

  @Override
  public void deactivate(final AuditSpan span) {
    activate(getUnbondedSpan());
  }

  /**
   * A static source of no-op spans, using the same span ID
   * source as managed spans.
   * @param name operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   */
  public static AuditSpan createNewSpan(
      final String name,
      final String path1,
      final String path2) {
    return NOOP_AUDITOR.createSpan(name, path1, path2);
  }
}
