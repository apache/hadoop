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
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestShuffleExceptionCount {

  static boolean abortCalled = false;
  private final float epsilon = 1e-5f;

  @BeforeClass
  public static void initialize() throws Exception {
    abortCalled = false;
  }
    
  public static class TestShuffleExceptionTracker extends ShuffleExceptionTracker {
    private static final long serialVersionUID = 1L;

    TestShuffleExceptionTracker(int size, String exceptionStackRegex,
        String exceptionMsgRegex, float shuffleExceptionLimit) {
      super(size, exceptionStackRegex,
          exceptionMsgRegex, shuffleExceptionLimit);
    }

    protected void doAbort() {
      abortCalled = true;
  }
  }

  @Test
  public void testCheckException() throws IOException, InterruptedException {

    // first test with only MsgRegex set but doesn't match
    String exceptionMsgRegex = "Broken pipe";
    String exceptionStackRegex = null;
    TestShuffleExceptionTracker shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0f);
    IOException ie = new IOException("EOFException");
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with only MsgRegex set that does match
    ie = new IOException("Broken pipe");
    exceptionStackRegex = null;
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with neither set, make sure incremented
    exceptionMsgRegex = null;
    exceptionStackRegex = null;
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with only StackRegex set doesn't match
    exceptionMsgRegex = null;
    exceptionStackRegex = ".*\\.doesnt\\$SelectSet\\.wakeup.*";
    ie.setStackTrace(constructStackTrace());
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with only StackRegex set does match
    exceptionMsgRegex = null;
    exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with both regex set and matches
    exceptionMsgRegex = "Broken pipe";
    ie.setStackTrace(constructStackTraceTwo());
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with both regex set and only msg matches
    exceptionStackRegex = ".*[1-9]+BOGUSREGEX";
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    // test with both regex set and only stack matches
    exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    exceptionMsgRegex = "EOFException";
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);

    exceptionMsgRegex = "Broken pipe";
    ie.setStackTrace(constructStackTraceTwo());
    shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);
  }

  @Test
  public void testExceptionCount() {
    String exceptionMsgRegex = "Broken pipe";
    String exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    IOException ie = new IOException("Broken pipe");
    ie.setStackTrace(constructStackTraceTwo());

    TestShuffleExceptionTracker shuffleExceptionTracker = new TestShuffleExceptionTracker(
        10, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);
    assertEquals("shuffleExceptionCount wrong", (float) 1 / (float) 10,
        shuffleExceptionTracker.getPercentExceptions(), epsilon);

    ie.setStackTrace(constructStackTraceThree());
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);
    assertEquals("shuffleExceptionCount wrong", (float) 1 / (float) 10,
        shuffleExceptionTracker.getPercentExceptions(), epsilon);

    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);
    assertEquals("shuffleExceptionCount wrong", (float) 1 / (float) 10,
        shuffleExceptionTracker.getPercentExceptions(), epsilon);

    ie.setStackTrace(constructStackTrace());
    shuffleExceptionTracker.checkException(ie);
    assertFalse("abort called when set to off", abortCalled);
    assertEquals("shuffleExceptionCount wrong", (float) 2 / (float) 10,
        shuffleExceptionTracker.getPercentExceptions(), epsilon);

    shuffleExceptionTracker.checkException(ie);
    assertTrue("abort not called", abortCalled);
    assertEquals("shuffleExceptionCount wrong", (float) 3 / (float) 10,
        shuffleExceptionTracker.getPercentExceptions(), epsilon);

  }

  @Test
  public void testShuffleExceptionTrailing() {
    String exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    String exceptionMsgRegex = "Broken pipe";
    int size = 5;
    ShuffleExceptionTracker tracker = new ShuffleExceptionTracker(
        size, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    assertEquals(size, tracker.getNumRequests());
    assertEquals(0, tracker.getPercentExceptions(), 0);
    tracker.success();
    assertEquals(0, tracker.getPercentExceptions(), 0);
    tracker.exception();
    assertEquals((float) 1 / (float) size, tracker.getPercentExceptions(), epsilon);
    tracker.exception();
    tracker.exception();
    assertEquals((float) 3 / (float) size, tracker.getPercentExceptions(), epsilon);
    tracker.exception();
    tracker.exception();
    tracker.exception();
    tracker.exception();
    assertEquals((float) 5 / (float) size, tracker.getPercentExceptions(), epsilon);
    // make sure we push out old ones
    tracker.success();
    tracker.success();
    assertEquals((float) 3 / (float) size, tracker.getPercentExceptions(), epsilon);
    tracker.exception();
    tracker.exception();
    tracker.exception();
    tracker.exception();
    tracker.exception();
    assertEquals((float) 5 / (float) size, tracker.getPercentExceptions(), epsilon);
  }

  @Test
  public void testShuffleExceptionTrailingSize() {
    String exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    String exceptionMsgRegex = "Broken pipe";
    int size = 1000;
    ShuffleExceptionTracker tracker = new ShuffleExceptionTracker(
        size, exceptionStackRegex, exceptionMsgRegex, 0.3f);
    assertEquals(size, tracker.getNumRequests());
    tracker.success();
    tracker.success();
    tracker.exception();
    tracker.exception();
    assertEquals((float) 2 / (float) size, tracker.getPercentExceptions(),
        epsilon);
  }


  /*
   * Construction exception like:
   * java.io.IOException: Broken pipe at
   * sun.nio.ch.EPollArrayWrapper.interrupt(Native Method) at
   * sun.nio.ch.EPollArrayWrapper.interrupt(EPollArrayWrapper.java:256) at
   * sun.nio.ch.EPollSelectorImpl.wakeup(EPollSelectorImpl.java:175) at
   * org.mortbay.io.nio.SelectorManager$SelectSet.wakeup(SelectorManager.java:831) at
   * org.mortbay.io.nio.SelectorManager$SelectSet.doSelect(SelectorManager.java:709) at
   * org.mortbay.io.nio.SelectorManager.doSelect(SelectorManager.java:192) at
   * org.mortbay.jetty.nio.SelectChannelConnector.accept(SelectChannelConnector.java:124) at
   * org.mortbay.jetty.AbstractConnector$Acceptor.run(AbstractConnector.java:708) at
   * org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)
   */
  private StackTraceElement[] constructStackTrace() {
    StackTraceElement[] stack = new StackTraceElement[9];
    stack[0] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "", -2);
    stack[1] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "EPollArrayWrapper.java", 256);
    stack[2] = new StackTraceElement("sun.nio.ch.EPollSelectorImpl", "wakeup", "EPollSelectorImpl.java", 175);
    stack[3] = new StackTraceElement("org.mortbay.io.nio.SelectorManager$SelectSet", "wakeup", "SelectorManager.java", 831);
    stack[4] = new StackTraceElement("org.mortbay.io.nio.SelectorManager$SelectSet", "doSelect", "SelectorManager.java", 709);
    stack[5] = new StackTraceElement("org.mortbay.io.nio.SelectorManager", "doSelect", "SelectorManager.java", 192);
    stack[6] = new StackTraceElement("org.mortbay.jetty.nio.SelectChannelConnector", "accept", "SelectChannelConnector.java", 124);
    stack[7] = new StackTraceElement("org.mortbay.jetty.AbstractConnector$Acceptor", "run", "AbstractConnector.java", 708);
    stack[8] = new StackTraceElement("org.mortbay.thread.QueuedThreadPool$PoolThread", "run", "QueuedThreadPool.java", 582);

    return stack;
  }

  /*
   * java.io.IOException: Broken pipe at
   * sun.nio.ch.EPollArrayWrapper.interrupt(Native Method) at
   * sun.nio.ch.EPollArrayWrapper.interrupt(EPollArrayWrapper.java:256) at
   * sun.nio.ch.EPollSelectorImpl.wakeup(EPollSelectorImpl.java:175) at
   * org.mortbay.io.nio.SelectorManager$SelectSet.wakeup(SelectorManager.java:831) at
   * org.mortbay.io.nio.SelectChannelEndPoint.updateKey(SelectChannelEndPoint.java:335) at
   * org.mortbay.io.nio.SelectChannelEndPoint.blockWritable(SelectChannelEndPoint.java:278) at
   * org.mortbay.jetty.AbstractGenerator$Output.blockForOutput(AbstractGenerator.java:545) at
   * org.mortbay.jetty.AbstractGenerator$Output.flush(AbstractGenerator.java:572) at
   * org.mortbay.jetty.HttpConnection$Output.flush(HttpConnection.java:1012) at
   * org.mortbay.jetty.AbstractGenerator$Output.write(AbstractGenerator.java:651)at
   * org.mortbay.jetty.AbstractGenerator$Output.write(AbstractGenerator.java:580) at
   */
  private StackTraceElement[] constructStackTraceTwo() {
    StackTraceElement[] stack = new StackTraceElement[11];
    stack[0] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "", -2);
    stack[1] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "EPollArrayWrapper.java", 256);
    stack[2] = new StackTraceElement("sun.nio.ch.EPollSelectorImpl", "wakeup", "EPollSelectorImpl.java", 175);
    stack[3] = new StackTraceElement("org.mortbay.io.nio.SelectorManager$SelectSet", "wakeup", "SelectorManager.java", 831);
    stack[4] = new StackTraceElement("org.mortbay.io.nio.SelectChannelEndPoint", "updateKey", "SelectChannelEndPoint.java", 335);
    stack[5] = new StackTraceElement("org.mortbay.io.nio.SelectChannelEndPoint", "blockWritable", "SelectChannelEndPoint.java", 278);
    stack[6] = new StackTraceElement("org.mortbay.jetty.AbstractGenerator$Output", "blockForOutput", "AbstractGenerator.java", 545);
    stack[7] = new StackTraceElement("org.mortbay.jetty.AbstractGenerator$Output", "flush", "AbstractGenerator.java", 572);
    stack[8] = new StackTraceElement("org.mortbay.jetty.HttpConnection$Output", "flush", "HttpConnection.java", 1012);
    stack[9] = new StackTraceElement("org.mortbay.jetty.AbstractGenerator$Output", "write", "AbstractGenerator.java", 651);
    stack[10] = new StackTraceElement("org.mortbay.jetty.AbstractGenerator$Output", "write", "AbstractGenerator.java", 580);

    return stack;
  }

  /*
   * java.io.IOException: Broken pipe at
   * sun.nio.ch.EPollArrayWrapper.interrupt(Native Method) at
   * sun.nio.ch.EPollArrayWrapper.interrupt(EPollArrayWrapper.java:256) at
   * sun.nio.ch.EPollSelectorImpl.wakeup(EPollSelectorImpl.java:175) at
   */
  private StackTraceElement[] constructStackTraceThree() {
    StackTraceElement[] stack = new StackTraceElement[3];
    stack[0] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "", -2);
    stack[1] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "interrupt", "EPollArrayWrapper.java", 256);
    stack[2] = new StackTraceElement("sun.nio.ch.EPollSelectorImpl", "wakeup", "EPollSelectorImpl.java", 175);

    return stack;
}
}
