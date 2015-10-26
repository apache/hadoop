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

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Assume;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;

/**
 * Test provides some very generic helpers which might be used across the tests
 */
public abstract class GenericTestUtils {

  private static final AtomicInteger sequence = new AtomicInteger();

  @SuppressWarnings("unchecked")
  public static void disableLog(Log log) {
    // We expect that commons-logging is a wrapper around Log4j.
    disableLog((Log4JLogger) log);
  }

  public static Logger toLog4j(org.slf4j.Logger logger) {
    return LogManager.getLogger(logger.getName());
  }

  public static void disableLog(Log4JLogger log) {
    log.getLogger().setLevel(Level.OFF);
  }

  public static void disableLog(Logger logger) {
    logger.setLevel(Level.OFF);
  }

  public static void disableLog(org.slf4j.Logger logger) {
    disableLog(toLog4j(logger));
  }

  @SuppressWarnings("unchecked")
  public static void setLogLevel(Log log, Level level) {
    // We expect that commons-logging is a wrapper around Log4j.
    setLogLevel((Log4JLogger) log, level);
  }

  public static void setLogLevel(Log4JLogger log, Level level) {
    log.getLogger().setLevel(level);
  }

  public static void setLogLevel(Logger logger, Level level) {
    logger.setLevel(level);
  }

  public static void setLogLevel(org.slf4j.Logger logger, Level level) {
    setLogLevel(toLog4j(logger), level);
  }

  /**
   * Extracts the name of the method where the invocation has happened
   * @return String name of the invoking method
   */
  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  /**
   * Generates a process-wide unique sequence number.
   * @return an unique sequence number
   */
  public static int uniqueSequenceId() {
    return sequence.incrementAndGet();
  }
  
  /**
   * Assert that a given file exists.
   */
  public static void assertExists(File f) {
    Assert.assertTrue("File " + f + " should exist", f.exists());
  }
    
  /**
   * List all of the files in 'dir' that match the regex 'pattern'.
   * Then check that this list is identical to 'expectedMatches'.
   * @throws IOException if the dir is inaccessible
   */
  public static void assertGlobEquals(File dir, String pattern,
      String ... expectedMatches) throws IOException {
    
    Set<String> found = Sets.newTreeSet();
    for (File f : FileUtil.listFiles(dir)) {
      if (f.getName().matches(pattern)) {
        found.add(f.getName());
      }
    }
    Set<String> expectedSet = Sets.newTreeSet(
        Arrays.asList(expectedMatches));
    Assert.assertEquals("Bad files matching " + pattern + " in " + dir,
        Joiner.on(",").join(expectedSet),
        Joiner.on(",").join(found));
  }

  protected static String E_NULL_THROWABLE = "Null Throwable";
  protected static String E_NULL_THROWABLE_STRING =
      "Null Throwable.toString() value";
  protected static String E_UNEXPECTED_EXCEPTION =
      "but got unexpected exception";

  /**
   * Assert that an exception's <code>toString()</code> value
   * contained the expected text.
   * @param string expected string
   * @param t thrown exception
   * @throws AssertionError if the expected string is not found
   */
  public static void assertExceptionContains(String string, Throwable t) {
    Assert.assertNotNull(E_NULL_THROWABLE, t);
    String msg = t.toString();
    if (msg == null) {
      throw new AssertionError(E_NULL_THROWABLE_STRING, t);
    }
    if (!msg.contains(string)) {
      throw new AssertionError("Expected to find '" + string + "' "
          + E_UNEXPECTED_EXCEPTION + ":"
          + StringUtils.stringifyException(t),
          t);
    }
  }  

  public static void waitFor(Supplier<Boolean> check,
      int checkEveryMillis, int waitForMillis)
      throws TimeoutException, InterruptedException
  {
    long st = Time.now();
    do {
      boolean result = check.get();
      if (result) {
        return;
      }
      
      Thread.sleep(checkEveryMillis);
    } while (Time.now() - st < waitForMillis);
    
    throw new TimeoutException("Timed out waiting for condition. " +
        "Thread diagnostics:\n" +
        TimedOutTestsListener.buildThreadDiagnosticString());
  }
  
  public static class LogCapturer {
    private StringWriter sw = new StringWriter();
    private WriterAppender appender;
    private Logger logger;
    
    public static LogCapturer captureLogs(Log l) {
      Logger logger = ((Log4JLogger)l).getLogger();
      LogCapturer c = new LogCapturer(logger);
      return c;
    }
    
    public static LogCapturer captureLogs(Logger l) {
      return new LogCapturer(l);
    }

    private LogCapturer(Logger logger) {
      this.logger = logger;
      Layout layout = Logger.getRootLogger().getAppender("stdout").getLayout();
      WriterAppender wa = new WriterAppender(layout, sw);
      logger.addAppender(wa);
    }
    
    public String getOutput() {
      return sw.toString();
    }
    
    public void stopCapturing() {
      logger.removeAppender(appender);

    }

    public void clearOutput() {
      sw.getBuffer().setLength(0);
    }
  }
  
  
  /**
   * Mockito answer helper that triggers one latch as soon as the
   * method is called, then waits on another before continuing.
   */
  public static class DelayAnswer implements Answer<Object> {
    private final Log LOG;
    
    private final CountDownLatch fireLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private final CountDownLatch resultLatch = new CountDownLatch(1);
    
    private final AtomicInteger fireCounter = new AtomicInteger(0);
    private final AtomicInteger resultCounter = new AtomicInteger(0);
    
    // Result fields set after proceed() is called.
    private volatile Throwable thrown;
    private volatile Object returnValue;
    
    public DelayAnswer(Log log) {
      this.LOG = log;
    }

    /**
     * Wait until the method is called.
     */
    public void waitForCall() throws InterruptedException {
      fireLatch.await();
    }
  
    /**
     * Tell the method to proceed.
     * This should only be called after waitForCall()
     */
    public void proceed() {
      waitLatch.countDown();
    }
  
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      LOG.info("DelayAnswer firing fireLatch");
      fireCounter.getAndIncrement();
      fireLatch.countDown();
      try {
        LOG.info("DelayAnswer waiting on waitLatch");
        waitLatch.await();
        LOG.info("DelayAnswer delay complete");
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted waiting on latch", ie);
      }
      return passThrough(invocation);
    }

    protected Object passThrough(InvocationOnMock invocation) throws Throwable {
      try {
        Object ret = invocation.callRealMethod();
        returnValue = ret;
        return ret;
      } catch (Throwable t) {
        thrown = t;
        throw t;
      } finally {
        resultCounter.incrementAndGet();
        resultLatch.countDown();
      }
    }
    
    /**
     * After calling proceed(), this will wait until the call has
     * completed and a result has been returned to the caller.
     */
    public void waitForResult() throws InterruptedException {
      resultLatch.await();
    }
    
    /**
     * After the call has gone through, return any exception that
     * was thrown, or null if no exception was thrown.
     */
    public Throwable getThrown() {
      return thrown;
    }
    
    /**
     * After the call has gone through, return the call's return value,
     * or null in case it was void or an exception was thrown.
     */
    public Object getReturnValue() {
      return returnValue;
    }
    
    public int getFireCount() {
      return fireCounter.get();
    }
    
    public int getResultCount() {
      return resultCounter.get();
    }
  }
  
  /**
   * An Answer implementation that simply forwards all calls through
   * to a delegate.
   * 
   * This is useful as the default Answer for a mock object, to create
   * something like a spy on an RPC proxy. For example:
   * <code>
   *    NamenodeProtocol origNNProxy = secondary.getNameNode();
   *    NamenodeProtocol spyNNProxy = Mockito.mock(NameNodeProtocol.class,
   *        new DelegateAnswer(origNNProxy);
   *    doThrow(...).when(spyNNProxy).getBlockLocations(...);
   *    ...
   * </code>
   */
  public static class DelegateAnswer implements Answer<Object> { 
    private final Object delegate;
    private final Log log;
    
    public DelegateAnswer(Object delegate) {
      this(null, delegate);
    }
    
    public DelegateAnswer(Log log, Object delegate) {
      this.log = log;
      this.delegate = delegate;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      try {
        if (log != null) {
          log.info("Call to " + invocation + " on " + delegate,
              new Exception("TRACE"));
        }
        return invocation.getMethod().invoke(
            delegate, invocation.getArguments());
      } catch (InvocationTargetException ite) {
        throw ite.getCause();
      }
    }
  }

  /**
   * An Answer implementation which sleeps for a random number of milliseconds
   * between 0 and a configurable value before delegating to the real
   * implementation of the method. This can be useful for drawing out race
   * conditions.
   */
  public static class SleepAnswer implements Answer<Object> {
    private final int maxSleepTime;
    private static Random r = new Random();
    
    public SleepAnswer(int maxSleepTime) {
      this.maxSleepTime = maxSleepTime;
    }
    
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      boolean interrupted = false;
      try {
        Thread.sleep(r.nextInt(maxSleepTime));
      } catch (InterruptedException ie) {
        interrupted = true;
      }
      try {
        return invocation.callRealMethod();
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public static void assertDoesNotMatch(String output, String pattern) {
    Assert.assertFalse("Expected output to match /" + pattern + "/" +
        " but got:\n" + output,
        Pattern.compile(pattern).matcher(output).find());
  }

  public static void assertMatches(String output, String pattern) {
    Assert.assertTrue("Expected output to match /" + pattern + "/" +
        " but got:\n" + output,
        Pattern.compile(pattern).matcher(output).find());
  }
  
  public static void assertValueNear(long expected, long actual, long allowedError) {
    assertValueWithinRange(expected - allowedError, expected + allowedError, actual);
  }
  
  public static void assertValueWithinRange(long expectedMin, long expectedMax,
      long actual) {
    Assert.assertTrue("Expected " + actual + " to be in range (" + expectedMin + ","
        + expectedMax + ")", expectedMin <= actual && actual <= expectedMax);
  }

  /**
   * Determine if there are any threads whose name matches the regex.
   * @param pattern a Pattern object used to match thread names
   * @return true if there is any thread that matches the pattern
   */
  public static boolean anyThreadMatching(Pattern pattern) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    ThreadInfo[] infos =
        threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null)
        continue;
      if (pattern.matcher(info.getThreadName()).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Assert that there are no threads running whose name matches the
   * given regular expression.
   * @param regex the regex to match against
   */
  public static void assertNoThreadsMatching(String regex) {
    Pattern pattern = Pattern.compile(regex);
    if (anyThreadMatching(pattern)) {
      Assert.fail("Leaked thread matches " + regex);
    }
  }

  /**
   * Periodically check and wait for any threads whose name match the
   * given regular expression.
   *
   * @param regex the regex to match against.
   * @param checkEveryMillis time (in milliseconds) between checks.
   * @param waitForMillis total time (in milliseconds) to wait before throwing
   *                      a time out exception.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public static void waitForThreadTermination(String regex,
      int checkEveryMillis, final int waitForMillis) throws TimeoutException,
      InterruptedException {
    final Pattern pattern = Pattern.compile(regex);
    waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        return !anyThreadMatching(pattern);
      }
    }, checkEveryMillis, waitForMillis);
  }


  /**
   * Skip test if native build profile of Maven is not activated.
   * Sub-project using this must set 'runningWithNative' property to true
   * in the definition of native profile in pom.xml.
   */
  public static void assumeInNativeProfile() {
    Assume.assumeTrue(
        Boolean.valueOf(System.getProperty("runningWithNative", "false")));
  }
}
