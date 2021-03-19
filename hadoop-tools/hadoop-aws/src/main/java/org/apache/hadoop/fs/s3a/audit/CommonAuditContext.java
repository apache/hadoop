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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.hadoop.fs.s3a.audit.AuditConstants.PROCESS;
import static org.apache.hadoop.fs.s3a.audit.AuditConstants.THREAD1;

/**
 * The common audit context is a map of common context information
 * which can be used with any audit span.
 * This context is shared across all S3A Filesystems within the
 * thread, irrespective of who the UGI owner is.
 * Audit spans will be created with a reference to the current
 * context of their thread;
 * That reference is retained even as they are moved across threads, so
 * context information (including initial thread ID from
 * {@link #THREAD_ID_COUNTER}) can be retrieved/used.
 */
public final class CommonAuditContext {

  private CommonAuditContext() {
  }

  /**
   * Process ID; currently built from UUID and timestamp.
   */
  public static final String PROCESS_ID;

  static {
    final LocalDateTime now = LocalDateTime.now();
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral('.')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .appendLiteral('.')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .toFormatter();
    PROCESS_ID = UUID.randomUUID().toString() + "-" + now.format(formatter);
  }

  /**
   * Counter of thread IDs.
   */
  private static final AtomicLong THREAD_ID_COUNTER = new AtomicLong();

  /**
   * Map of data. Concurrent so when shared across threads
   * there are no problems.
   * Supplier operations must themselves be threadsafe.
   */
  private final Map<String, Supplier<String>> evaluatedOperations =
      new ConcurrentHashMap<>();

  /**
   * Put a context entry.
   * @param key key
   * @param value new value
   * @return old value or null
   */
  public Supplier<String> put(String key, String value) {
    return evaluatedOperations.put(key, () -> value);
  }

  /**
   * Put a context entry dynamically evaluated on demand.
   * @param key key
   * @param value new value
   * @return old value or null
   */
  public Supplier<String> put(String key, Supplier<String> value) {
    return evaluatedOperations.put(key, value);
  }

  /**
   * Remove a context entry.
   * @param key key
   */
  public void remove(String key) {
    evaluatedOperations.remove(key);
  }

  /**
   * Get a context entry.
   * @param key key
   * @return value or null
   */
  public String get(String key) {
    return evaluatedOperations.get(key).get();
  }

  /**
   * Does the context contain a specific key?
   * @param key key
   * @return true if it is in the context.
   */
  public boolean containsKey(String key) {
    return evaluatedOperations.containsKey(key);
  }

  /**
   * Thread local context.
   * Use a weak reference just to keep memory costs down.
   * The S3A committers all have a strong reference, so if they are
   * retained, context is retained.
   * If a span retains the context, then it will also stay valid until
   * the span is finalized.
   */
  private static final ThreadLocal<CommonAuditContext> ACTIVE_CONTEXT =
      ThreadLocal.withInitial(() -> createInstance());

  /**
   * Thread local thread ID.
   */
  private static final ThreadLocal<String> THREAD_ID =
      ThreadLocal.withInitial(() -> String.format("%04x", nextThreadId()));

  /**
   * Demand invoked to create the instance for this thread.
   * @return an instance.
   */
  private static CommonAuditContext createInstance() {
    CommonAuditContext context = new CommonAuditContext();
    // process ID is fixed.
    context.put(PROCESS, PROCESS_ID);
    // thread 1 is dynamic
    context.put(THREAD1, () -> currentThreadID());
    return context;
  }

  /**
   * Get the current common context. Thread local.
   * @return the context of this thread.
   */
  public static CommonAuditContext currentContext() {
    return ACTIVE_CONTEXT.get();
  }

  /**
   * Generate a new thread ID.
   * @return a number unique for this process.
   */
  private static long nextThreadId() {
    return THREAD_ID_COUNTER.incrementAndGet();
  }

  /**
   * A thread ID which is unique for this process and shared across all
   * S3A clients on the same thread, even those using different FS instances.
   * @return a thread ID for reporting.
   */
  public static String currentThreadID() {
    return THREAD_ID.get();
  }

  /**
   * Get the evaluated operations. This is the map
   * unique to this context.
   * @return the operations map.
   */
  public Map<String, Supplier<String>> getEvaluatedOperations() {
    return evaluatedOperations;
  }
}
