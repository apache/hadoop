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

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.audit.impl.AbstractAuditSpanImpl;
import org.apache.hadoop.fs.s3a.audit.impl.AbstractOperationAuditor;


/**
 * An audit service which consumes lots of memory.
 */
public class MemoryHungryAuditor extends AbstractOperationAuditor {

  public static final String NAME = "org.apache.hadoop.fs.s3a.audit.MemoryHungryAuditor";

  private static final Logger LOG =
      LoggerFactory.getLogger(MemoryHungryAuditor.class);
  /**
   * How big is each manager?
   */
  public static final int MANAGER_SIZE = 10 * 1024 * 1024;

  /**
   * How big is each span?
   */
  public static final int SPAN_SIZE = 512 * 1024;

  private static final AtomicLong INSTANCE_COUNT = new AtomicLong();

  private final AtomicLong spanCount = new AtomicLong();

  private final byte[] data = new byte[MANAGER_SIZE];

  /**
   * unbonded span created on demand.
   */
  private AuditSpanS3A unbondedSpan;


  /**
   * Constructor.
   */
  public MemoryHungryAuditor() {
    super("MemoryHungryAuditor");
    INSTANCE_COUNT.incrementAndGet();
  }

  public long getSpanCount() {
    return spanCount.get();
  }

  @Override
  public AuditSpanS3A createSpan(
      final String operation,
      @Nullable final String path1,
      @Nullable final String path2) {
    spanCount.incrementAndGet();
    return new MemorySpan(createSpanID(), operation);
  }

  @Override
  public AuditSpanS3A getUnbondedSpan() {
    if (unbondedSpan == null) {
      unbondedSpan = new MemorySpan(createSpanID(), "unbonded");
    }
    return unbondedSpan;
  }

  @Override
  public String toString() {
    return String.format("%s instance %d span count %d",
        super.toString(),
        getInstanceCount(),
        getSpanCount());
  }

  @Override
  public void noteSpanReferenceLost(final long threadId) {
    LOG.info("Span lost for thread {}", threadId);
  }

  public static long getInstanceCount() {
    return INSTANCE_COUNT.get();
  }

  /**
   * A span which consumes a lot of memory.
   */
  private static final class MemorySpan extends AbstractAuditSpanImpl {

    private final byte[] data = new byte[SPAN_SIZE];

    private MemorySpan(final String spanId, final String operationName) {
      super(spanId, operationName);
    }

    @Override
    public AuditSpanS3A activate() {
      return this;
    }

    @Override
    public void deactivate() {
    }

  }

}
