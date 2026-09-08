/**
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
package org.apache.hadoop.test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.StringUtils;

/**
 * JUnit Platform listener which prints a full thread dump into System.err
 * in case a test fails due to timeout.
 *
 * <p>Registered through the Surefire {@code listener} provider property,
 * which eight module poms carry. That property registers nothing at
 * present: the JUnit Platform provider ignores it, which is why the dumps
 * stopped at the JUnit 5 migration. It is kept, and this class implements
 * {@link TestExecutionListener} rather than the JUnit 4 {@code RunListener}
 * it used to, so that the existing wiring starts working with no change to
 * Hadoop beyond a Surefire version bump once the provider accepts platform
 * listeners there (SUREFIRE-1639, apache/maven-surefire#3438).</p>
 *
 * <p>Registration through {@code META-INF/services} would work today and is
 * deliberately not used: the descriptor would ship in hadoop-common's test
 * artifact and auto-activate this listener in every downstream project that
 * puts that artifact on a JUnit Platform test classpath.</p>
 *
 * <p>Until the provider catches up, the dump Hadoop's tests actually get
 * comes from {@link #dumpForTimeout}, which {@link GenericTestUtils#waitFor}
 * calls directly and which needs no registration at all.</p>
 *
 * <p>Once active, this listener detects the timeout failures thrown by
 * JUnit 5 {@code @Timeout} (a {@link TimeoutException}), by the JUnit 4
 * vintage runner ({@code TestTimedOutException}), and by any other failure
 * whose message contains "timed out after". A timeout that dumped for
 * itself through {@link #dumpForTimeout} says so in its message, and this
 * listener stays quiet for it, so a timeout yields exactly one dump.</p>
 *
 * <p>Scope: it can only fire for timeouts that surface through JUnit — an
 * explicit or default {@code @Timeout}. It cannot fire when Surefire kills
 * the fork at {@code forkedProcessTimeoutInSeconds}: the plugin sends the
 * fork {@code Shutdown.KILL}, which executes {@code Runtime.halt()} and
 * bypasses listeners and shutdown hooks alike. Diagnostics for a fork that
 * fails to <em>exit</em> after the tests complete are produced by Surefire
 * itself and captured in CI since HADOOP-19950.</p>
 *
 * <p>Set {@code -Dhadoop.test.timedout.dump=false} to disable the dump,
 * and {@code -Dhadoop.test.timedout.dump.limit} (default 5) to bound the
 * number of dumps a single JVM prints. Both entry points — this listener
 * and {@link #dumpForTimeout} — obey the switch and share the one budget.</p>
 */
public class TimedOutTestsListener implements TestExecutionListener {

  private static final String TIMED_OUT_MARKER = "timed out after";

  /**
   * Sentence a caller of {@link #dumpForTimeout} appends to the exception it
   * throws, to record that a dump has already been printed for that failure.
   */
  static final String DUMP_PRINTED_MARKER = "Thread dump printed to stderr.";

  private static final String JUNIT4_TIMEOUT_EXCEPTION =
      "org.junit.runners.model.TestTimedOutException";

  /** Set to "false" to disable thread dumps entirely. */
  static final String DUMP_PROPERTY = "hadoop.test.timedout.dump";

  /**
   * Maximum thread dumps a single JVM prints; further ones are elided. This
   * listener and {@link #dumpForTimeout} draw on one shared budget, so a JVM
   * that has spent it on waitFor timeouts will not dump for a later
   * {@code @Timeout} failure.
   */
  static final String DUMP_LIMIT_PROPERTY = "hadoop.test.timedout.dump.limit";

  private static final int DEFAULT_DUMP_LIMIT = 5;

  private static final AtomicInteger DUMPS = new AtomicInteger();

  private static final String INDENT = "    ";

  private final PrintWriter output;

  public TimedOutTestsListener() {
    // System.err is captured once, deliberately: it pins the real Surefire
    // stderr, so a test that leaves System.err redirected cannot swallow the
    // dump.
    this.output = new PrintWriter(System.err);
  }

  public TimedOutTestsListener(PrintWriter output) {
    this.output = output;
  }

  @Override
  public void executionFinished(TestIdentifier testIdentifier,
      TestExecutionResult testExecutionResult) {
    if (testExecutionResult.getStatus()
        != TestExecutionResult.Status.FAILED) {
      return;
    }
    try {
      Throwable failure =
          testExecutionResult.getThrowable().orElse(null);
      // shouldDump() is checked last: it consumes dump budget, so a
      // suppressed dump must not spend any.
      if (isTimeoutFailure(failure) && !dumpAlreadyPrinted(failure)
          && shouldDump()) {
        printThreadDump("Test: " + testIdentifier.getDisplayName());
      }
    } catch (RuntimeException e) {
      // Diagnostics must never fail the run.
    }
  }

  /**
   * Whether a dump should be printed now: the feature is enabled and this
   * JVM's dump limit has not been exhausted. Prints a single elision
   * notice when the limit is first exceeded.
   */
  boolean shouldDump() {
    if (!Boolean.parseBoolean(System.getProperty(DUMP_PROPERTY, "true"))) {
      return false;
    }
    int limit = Integer.getInteger(DUMP_LIMIT_PROPERTY, DEFAULT_DUMP_LIMIT);
    int count = DUMPS.incrementAndGet();
    if (count > limit) {
      if (count == limit + 1) {
        output.println("====> TEST TIMED OUT. Thread dump elided: "
            + limit + " dumps already printed by this JVM. <====");
        output.flush();
      }
      return false;
    }
    return true;
  }

  @VisibleForTesting
  static void resetDumpCountForTesting() {
    DUMPS.set(0);
  }

  /**
   * Whether the given failure is a test timeout.
   */
  static boolean isTimeoutFailure(Throwable failure) {
    if (failure == null) {
      return false;
    }
    if (failure instanceof TimeoutException) {
      return true;
    }
    if (JUNIT4_TIMEOUT_EXCEPTION.equals(failure.getClass().getName())) {
      return true;
    }
    String message = failure.getMessage();
    return message != null && message.contains(TIMED_OUT_MARKER);
  }

  /**
   * Whether a dump was already printed for this failure when the timeout was
   * detected, as {@link GenericTestUtils#waitFor} does. That dump is the
   * better one — taken while the threads were still hung, rather than here,
   * after the test method and its teardown have unwound — so this listener
   * adds nothing.
   */
  static boolean dumpAlreadyPrinted(Throwable failure) {
    if (failure == null) {
      return false;
    }
    String message = failure.getMessage();
    return message != null && message.contains(DUMP_PRINTED_MARKER);
  }

  /**
   * Print a thread dump for a timeout detected before JUnit reports the
   * failure, so the threads are captured while still hung. Honours the same
   * {@value #DUMP_PROPERTY} switch and per-JVM limit as the listener itself,
   * so it does not always print.
   *
   * <p>A caller that gets {@code true} back should append
   * {@link #DUMP_PRINTED_MARKER} to the exception it throws, so a listener
   * does not print a second dump for the same failure. On {@code false} it
   * must not: nothing was printed, and the same switch and budget that
   * refused here will refuse the listener too, so there is no second dump to
   * suppress.</p>
   *
   * @param context what timed out, printed above the dump.
   * @return whether a dump was printed.
   */
  public static boolean dumpForTimeout(String context) {
    TimedOutTestsListener listener = new TimedOutTestsListener();
    if (!listener.shouldDump()) {
      return false;
    }
    listener.printThreadDump("Timed out in: " + context);
    return true;
  }

  /**
   * @param context complete line naming what timed out, e.g. "Test: foo()".
   */
  void printThreadDump(String context) {
    output.println("====> TEST TIMED OUT. PRINTING THREAD DUMP. <====");
    if (context != null) {
      output.println(context);
    }
    output.println();
    output.print(buildThreadDiagnosticString());
    output.flush();
  }

  public static String buildThreadDiagnosticString() {
    StringWriter sw = new StringWriter();
    PrintWriter output = new PrintWriter(sw);

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    output.println(String.format("Timestamp: %s", dateFormat.format(new Date())));
    output.println();
    output.println(buildThreadDump());

    String deadlocksInfo = buildDeadlockInfo();
    if (deadlocksInfo != null) {
      output.println("====> DEADLOCKS DETECTED <====");
      output.println();
      output.println(deadlocksInfo);
    }

    return sw.toString();
  }

  static String buildThreadDump() {
    StringBuilder dump = new StringBuilder();
    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
      Thread thread = e.getKey();
      dump.append(String.format(
          "\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s",
          thread.getName(),
          (thread.isDaemon() ? "daemon" : ""),
          thread.getPriority(),
          thread.getId(),
          Thread.State.WAITING.equals(thread.getState()) ?
              "in Object.wait()" :
              StringUtils.toLowerCase(thread.getState().name()),
          Thread.State.WAITING.equals(thread.getState()) ?
              "WAITING (on object monitor)" : thread.getState()));
      for (StackTraceElement stackTraceElement : e.getValue()) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n");
    }
    return dump.toString();
  }

  static String buildDeadlockInfo() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    long[] threadIds = threadBean.findMonitorDeadlockedThreads();
    if (threadIds != null && threadIds.length > 0) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter out = new PrintWriter(stringWriter);

      ThreadInfo[] infos = threadBean.getThreadInfo(threadIds, true, true);
      for (ThreadInfo ti : infos) {
        printThreadInfo(ti, out);
        printLockInfo(ti.getLockedSynchronizers(), out);
        out.println();
      }

      out.close();
      return stringWriter.toString();
    } else {
      return null;
    }
  }

  private static void printThreadInfo(ThreadInfo ti, PrintWriter out) {
    // print thread information
    printThread(ti, out);

    // print stack trace with locks
    StackTraceElement[] stacktrace = ti.getStackTrace();
    MonitorInfo[] monitors = ti.getLockedMonitors();
    for (int i = 0; i < stacktrace.length; i++) {
      StackTraceElement ste = stacktrace[i];
      out.println(INDENT + "at " + ste.toString());
      for (MonitorInfo mi : monitors) {
        if (mi.getLockedStackDepth() == i) {
          out.println(INDENT + "  - locked " + mi);
        }
      }
    }
    out.println();
  }

  private static void printThread(ThreadInfo ti, PrintWriter out) {
    out.print("\"" + ti.getThreadName() + "\"" + " Id="
        + ti.getThreadId() + " in " + ti.getThreadState());
    if (ti.getLockName() != null) {
      out.print(" on lock=" + ti.getLockName());
    }
    if (ti.isSuspended()) {
      out.print(" (suspended)");
    }
    if (ti.isInNative()) {
      out.print(" (running in native)");
    }
    out.println();
    if (ti.getLockOwnerName() != null) {
      out.println(INDENT + " owned by " + ti.getLockOwnerName() + " Id="
          + ti.getLockOwnerId());
    }
  }

  private static void printLockInfo(LockInfo[] locks, PrintWriter out) {
    out.println(INDENT + "Locked synchronizers: count = " + locks.length);
    for (LockInfo li : locks) {
      out.println(INDENT + "  - " + li);
    }
    out.println();
  }

}
