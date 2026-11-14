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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimedOutTestsListener {

  public static class Deadlock {
    private CyclicBarrier barrier = new CyclicBarrier(6);
  
    public Deadlock() {
      DeadlockThread[] dThreads = new DeadlockThread[6];
  
      Monitor a = new Monitor("a");
      Monitor b = new Monitor("b");
      Monitor c = new Monitor("c");
      dThreads[0] = new DeadlockThread("MThread-1", a, b);
      dThreads[1] = new DeadlockThread("MThread-2", b, c);
      dThreads[2] = new DeadlockThread("MThread-3", c, a);
  
      Lock d = new ReentrantLock();
      Lock e = new ReentrantLock();
      Lock f = new ReentrantLock();
  
      dThreads[3] = new DeadlockThread("SThread-4", d, e);
      dThreads[4] = new DeadlockThread("SThread-5", e, f);
      dThreads[5] = new DeadlockThread("SThread-6", f, d);
  
      // make them daemon threads so that the test will exit
      for (int i = 0; i < 6; i++) {
        dThreads[i].setDaemon(true);
        dThreads[i].start();
      }
    }
  
    class DeadlockThread extends Thread {
      private Lock lock1 = null;
  
      private Lock lock2 = null;
  
      private Monitor mon1 = null;
  
      private Monitor mon2 = null;
  
      private boolean useSync;
  
      DeadlockThread(String name, Lock lock1, Lock lock2) {
        super(name);
        this.lock1 = lock1;
        this.lock2 = lock2;
        this.useSync = true;
      }
  
      DeadlockThread(String name, Monitor mon1, Monitor mon2) {
        super(name);
        this.mon1 = mon1;
        this.mon2 = mon2;
        this.useSync = false;
      }
  
      public void run() {
        if (useSync) {
          syncLock();
        } else {
          monitorLock();
        }
      }
  
      private void syncLock() {
        lock1.lock();
        try {
          try {
            barrier.await();
          } catch (Exception e) {
          }
          goSyncDeadlock();
        } finally {
          lock1.unlock();
        }
      }
  
      private void goSyncDeadlock() {
        try {
          barrier.await();
        } catch (Exception e) {
        }
        lock2.lock();
        throw new RuntimeException("should not reach here.");
      }
  
      private void monitorLock() {
        synchronized (mon1) {
          try {
            barrier.await();
          } catch (Exception e) {
          }
          goMonitorDeadlock();
        }
      }
  
      private void goMonitorDeadlock() {
        try {
          barrier.await();
        } catch (Exception e) {
        }
        synchronized (mon2) {
          throw new RuntimeException(getName() + " should not reach here.");
        }
      }
    }
  
    class Monitor {
      private final String name;

      Monitor(String name) {
        this.name = name;
      }

      @Override
      public String toString() {
        return name;
      }
    }
  
  }

  @Test
  @Timeout(value = 30)
  public void testThreadDumpAndDeadlocks() throws Exception {
    new Deadlock();
    String s = null;
    while (true) {
      s = TimedOutTestsListener.buildDeadlockInfo();
      if (s != null) {
        break;
      }
      Thread.sleep(100);
    }
    
    assertEquals(3, countStringOccurrences(s, "BLOCKED"));
    
    RuntimeException failure =
        new RuntimeException("test timed out after 1000 milliseconds");
    assertTrue(TimedOutTestsListener.isTimeoutFailure(failure));
    StringWriter writer = new StringWriter();
    new TimedOutTestsListener(new PrintWriter(writer))
        .printThreadDump("testThreadDumpAndDeadlocks()");
    String out = writer.toString();
    
    assertTrue(out.contains("THREAD DUMP"));
    assertTrue(out.contains("DEADLOCKS DETECTED"));
    
    System.out.println(out);
  }

  @Test
  @Timeout(value = 30)
  public void testDumpDisabledByProperty() {
    TimedOutTestsListener.resetDumpCountForTesting();
    System.setProperty(TimedOutTestsListener.DUMP_PROPERTY, "false");
    try {
      StringWriter writer = new StringWriter();
      assertFalse(
          new TimedOutTestsListener(new PrintWriter(writer)).shouldDump());
      assertEquals("", writer.toString());
    } finally {
      System.clearProperty(TimedOutTestsListener.DUMP_PROPERTY);
    }
  }

  @Test
  @Timeout(value = 30)
  public void testDumpLimit() {
    TimedOutTestsListener.resetDumpCountForTesting();
    System.setProperty(TimedOutTestsListener.DUMP_LIMIT_PROPERTY, "2");
    try {
      StringWriter writer = new StringWriter();
      TimedOutTestsListener listener =
          new TimedOutTestsListener(new PrintWriter(writer));
      assertTrue(listener.shouldDump());
      assertTrue(listener.shouldDump());
      // Third dump exceeds the limit: refused, with a single elision notice.
      assertFalse(listener.shouldDump());
      assertTrue(writer.toString().contains("Thread dump elided"));
      // Fourth is refused silently.
      int len = writer.toString().length();
      assertFalse(listener.shouldDump());
      assertEquals(len, writer.toString().length());
    } finally {
      System.clearProperty(TimedOutTestsListener.DUMP_LIMIT_PROPERTY);
      TimedOutTestsListener.resetDumpCountForTesting();
    }
  }

  /**
   * GenericTestUtils.waitFor prints its own dump when the wait expires, while
   * the threads are still hung, and records that in the exception it throws
   * so the listener stays quiet. Driving the real waitFor keeps this honest
   * if either side ever changes.
   */
  @Test
  @Timeout(value = 30)
  public void testWaitForPrintsItsOwnDump() throws Exception {
    TimedOutTestsListener.resetDumpCountForTesting();
    PrintStream oldErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    TimeoutException failure;
    try {
      System.setErr(new PrintStream(captured, true));
      failure = assertThrows(TimeoutException.class,
          () -> GenericTestUtils.waitFor(() -> false, 10, 50, "still false"));
    } finally {
      System.setErr(oldErr);
      TimedOutTestsListener.resetDumpCountForTesting();
    }

    // The dump goes to stderr, at the moment the wait expired.
    String dump = captured.toString();
    assertTrue(dump.contains("PRINTING THREAD DUMP"));
    assertTrue(dump.contains("Timed out in: GenericTestUtils.waitFor"));

    // It no longer goes into the message, which used to carry tens of KB.
    String message = failure.getMessage();
    assertTrue(TimedOutTestsListener.isTimeoutFailure(failure));
    assertTrue(TimedOutTestsListener.dumpAlreadyPrinted(failure));
    assertFalse(message.contains("java.lang.Thread.State"));
    assertEquals("Timed out waiting for condition. Error Message: still false"
        + " Thread dump printed to stderr.", message);

    // A timeout carrying no dump of its own is still dumped for.
    assertFalse(TimedOutTestsListener.dumpAlreadyPrinted(
        new TimeoutException("test timed out after 1000 milliseconds")));
  }

  /**
   * The off switch now reaches waitFor too, which the inlined dump it used
   * to build was never subject to. With no dump printed, the exception must
   * not claim one was.
   */
  @Test
  @Timeout(value = 30)
  public void testWaitForDumpHonoursOffSwitch() throws Exception {
    TimedOutTestsListener.resetDumpCountForTesting();
    System.setProperty(TimedOutTestsListener.DUMP_PROPERTY, "false");
    PrintStream oldErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    TimeoutException failure;
    try {
      System.setErr(new PrintStream(captured, true));
      failure = assertThrows(TimeoutException.class,
          () -> GenericTestUtils.waitFor(() -> false, 10, 50, "no dump here"));
    } finally {
      System.setErr(oldErr);
      System.clearProperty(TimedOutTestsListener.DUMP_PROPERTY);
      TimedOutTestsListener.resetDumpCountForTesting();
    }
    assertEquals("", captured.toString());
    assertFalse(TimedOutTestsListener.dumpAlreadyPrinted(failure));
    assertEquals("Timed out waiting for condition. Error Message: no dump here",
        failure.getMessage());
  }

  /**
   * Same when the dump is refused because this JVM's budget is spent: the
   * marker would send the reader looking for a dump that is not there.
   */
  @Test
  @Timeout(value = 30)
  public void testWaitForDumpHonoursLimit() throws Exception {
    TimedOutTestsListener.resetDumpCountForTesting();
    System.setProperty(TimedOutTestsListener.DUMP_LIMIT_PROPERTY, "1");
    PrintStream oldErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    TimeoutException first;
    TimeoutException second;
    try {
      System.setErr(new PrintStream(captured, true));
      first = assertThrows(TimeoutException.class,
          () -> GenericTestUtils.waitFor(() -> false, 10, 50));
      second = assertThrows(TimeoutException.class,
          () -> GenericTestUtils.waitFor(() -> false, 10, 50));
    } finally {
      System.setErr(oldErr);
      System.clearProperty(TimedOutTestsListener.DUMP_LIMIT_PROPERTY);
      TimedOutTestsListener.resetDumpCountForTesting();
    }
    assertTrue(TimedOutTestsListener.dumpAlreadyPrinted(first));
    assertFalse(TimedOutTestsListener.dumpAlreadyPrinted(second));
    assertEquals("Timed out waiting for condition.", second.getMessage());
    assertTrue(captured.toString().contains("Thread dump elided"));
  }

  /**
   * dumpForTimeout reports whether it printed, which is what lets a caller
   * decide honestly whether to record the marker.
   */
  @Test
  @Timeout(value = 30)
  public void testDumpForTimeoutReportsWhetherItPrinted() {
    TimedOutTestsListener.resetDumpCountForTesting();
    PrintStream oldErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setErr(new PrintStream(captured, true));
      assertTrue(TimedOutTestsListener.dumpForTimeout("unit test"));
      System.setProperty(TimedOutTestsListener.DUMP_PROPERTY, "false");
      assertFalse(TimedOutTestsListener.dumpForTimeout("unit test"));
    } finally {
      System.setErr(oldErr);
      System.clearProperty(TimedOutTestsListener.DUMP_PROPERTY);
      TimedOutTestsListener.resetDumpCountForTesting();
    }
    assertEquals(1, countStringOccurrences(captured.toString(),
        "Timed out in: unit test"));
  }

  private int countStringOccurrences(String s, String substr) {
    int n = 0;
    int index = 0;
    while ((index = s.indexOf(substr, index) + 1) != 0) {
      n++;
    }
    return n;
  }

}
