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

package org.apache.hadoop.fs.audit;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_COMMAND;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_PROCESS;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD1;

/**
 * The common audit context is a map of common context information
 * which can be used with any audit span.
 * This context is shared across all Filesystems within the
 * thread.
 * Audit spans will be created with a reference to the current
 * context of their thread;
 * That reference is retained even as they are moved across threads, so
 * context information (including thread ID Java runtime).
 *
 * The Global context entries are a set of key-value pairs which span
 * all threads; the {@code HttpReferrerAuditHeader} picks these
 * up automatically. It is intended for minimal use of
 * shared constant values (process ID, entry point).
 *
 * An attribute set in {@link #setGlobalContextEntry(String, String)}
 * will be set across all audit spans in all threads.
 *
 * The {@link #noteEntryPoint(Object)} method should be
 * used in entry points (ToolRunner.run, etc). It extracts
 * the final element of the classname and attaches that
 * to the global context with the attribute key
 * {@link AuditConstants#PARAM_COMMAND}, if not already
 * set.
 * This helps identify the application being executued.
 *
 * All other values set are specific to this context, which
 * is thread local.
 * The attributes which can be added to ths common context include
 * evaluator methods which will be evaluated in whichever thread
 * invokes {@link #getEvaluatedEntries()} and then evaluates them.
 * That map of evaluated options may evaluated later, in a different
 * thread.
 *
 * For setting and clearing thread-level options, use
 * {@link #currentAuditContext()} to get the thread-local
 * context for the caller, which can then be manipulated.
 *
 * For further information, especially related to memory consumption,
 * read the document `auditing_architecture` in the `hadoop-aws` module.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class CommonAuditContext {

  private static final Logger LOG = LoggerFactory.getLogger(
      CommonAuditContext.class);

  /**
   * Process ID; currently built from UUID and timestamp.
   */
  public static final String PROCESS_ID = UUID.randomUUID().toString();

  /**
   * Context values which are global.
   * To be used very sparingly.
   */
  private static final Map<String, String> GLOBAL_CONTEXT_MAP =
      new ConcurrentHashMap<>();

  /**
   * Map of data. Concurrent so when shared across threads
   * there are no problems.
   * Supplier operations must themselves be thread safe.
   */
  private final Map<String, Supplier<String>> evaluatedEntries =
      new ConcurrentHashMap<>(1);

  static {
    // process ID is fixed.
    setGlobalContextEntry(PARAM_PROCESS, PROCESS_ID);
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
      ThreadLocal.withInitial(CommonAuditContext::createInstance);

  private CommonAuditContext() {
  }

  /**
   * Put a context entry.
   * @param key key
   * @param value new value., If null, triggers removal.
   * @return old value or null
   */
  public Supplier<String> put(String key, String value) {
    if (value != null) {
      return evaluatedEntries.put(key, () -> value);
    } else {
      return evaluatedEntries.remove(key);
    }
  }

  /**
   * Put a context entry dynamically evaluated on demand.
   * Important: as these supplier methods are long-lived,
   * the supplier function <i>MUST NOT</i> be part of/refer to
   * any object instance of significant memory size.
   * Applications SHOULD remove references when they are
   * no longer needed.
   * When logged at TRACE, prints the key and stack trace of the caller,
   * to allow for debugging of any problems.
   * @param key key
   * @param value new value
   * @return old value or null
   */
  public Supplier<String> put(String key, Supplier<String> value) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding context entry {}", key, new Exception(key));
    }
    return evaluatedEntries.put(key, value);
  }

  /**
   * Remove a context entry.
   * @param key key
   */
  public void remove(String key) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Remove context entry {}", key);
    }
    evaluatedEntries.remove(key);
  }

  /**
   * Get a context entry.
   * @param key key
   * @return value or null
   */
  public String get(String key) {
    Supplier<String> supplier = evaluatedEntries.get(key);
    return supplier != null
        ? supplier.get()
        : null;
  }

  /**
   * Rest the context; will set the standard options again.
   * Primarily for testing.
   */
  public void reset() {
    evaluatedEntries.clear();
    init();
  }

  /**
   * Initialize.
   */
  private void init() {

    // thread 1 is dynamic
    put(PARAM_THREAD1, CommonAuditContext::currentThreadID);
  }

  /**
   * Does the context contain a specific key?
   * @param key key
   * @return true if it is in the context.
   */
  public boolean containsKey(String key) {
    return evaluatedEntries.containsKey(key);
  }

  /**
   * Demand invoked to create the instance for this thread.
   * @return an instance.
   */
  private static CommonAuditContext createInstance() {
    CommonAuditContext context = new CommonAuditContext();
    context.init();
    return context;
  }

  /**
   * Get the current common audit context. Thread local.
   * @return the audit context of this thread.
   */
  public static CommonAuditContext currentAuditContext() {
    return ACTIVE_CONTEXT.get();
  }

  /**
   * A thread ID which is unique for this process and shared across all
   * S3A clients on the same thread, even those using different FS instances.
   * @return a thread ID for reporting.
   */
  public static String currentThreadID() {
    return Long.toString(Thread.currentThread().getId());
  }

  /**
   * Get the evaluated operations.
   * This is the map unique to this context.
   * @return the operations map.
   */
  public Map<String, Supplier<String>> getEvaluatedEntries() {
    return evaluatedEntries;
  }

  /**
   * Set a global entry.
   * @param key key
   * @param value value
   */
  public static void setGlobalContextEntry(String key, String value) {
    GLOBAL_CONTEXT_MAP.put(key, value);
  }

  /**
   * Get a global entry.
   * @param key key
   * @return value or null
   */
  public static String getGlobalContextEntry(String key) {
    return GLOBAL_CONTEXT_MAP.get(key);
  }

  /**
   * Remove a global entry.
   * @param key key to clear.
   */
  public static void removeGlobalContextEntry(String key) {
    GLOBAL_CONTEXT_MAP.remove(key);
  }

  /**
   * Add the entry point as a context entry with the key
   * {@link AuditConstants#PARAM_COMMAND}
   * if it has not  already been recorded.
   * This is called via ToolRunner but may be used at any
   * other entry point.
   * @param tool object loaded/being launched.
   */
  public static void noteEntryPoint(Object tool) {
    if (tool != null && !GLOBAL_CONTEXT_MAP.containsKey(PARAM_COMMAND)) {
      String classname = tool.getClass().toString();
      int lastDot = classname.lastIndexOf('.');
      int l = classname.length();
      if (lastDot > 0 && lastDot < (l - 1)) {
        String name = classname.substring(lastDot + 1, l);
        setGlobalContextEntry(PARAM_COMMAND, name);
      }
    }
  }

  /**
   * Get an iterator over the global entries.
   * Thread safe.
   * @return an iterable to enumerate the values.
   */
  public static Iterable<Map.Entry<String, String>>
      getGlobalContextEntries() {
    return new GlobalIterable();
  }

  /**
   * Iterable to the global iterator. Avoids serving
   * up full access to the map.
   */
  private static final class GlobalIterable
      implements Iterable<Map.Entry<String, String>> {

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return GLOBAL_CONTEXT_MAP.entrySet().iterator();
    }
  }

}
