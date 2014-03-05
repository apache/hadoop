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
package org.apache.hadoop.io.retry;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith;
import org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.Test;

public class TestFailoverProxy {

  public static class FlipFlopProxyProvider<T> implements FailoverProxyProvider<T> {
    
    private Class<T> iface;
    private T currentlyActive;
    private T impl1;
    private T impl2;
    
    private int failoversOccurred = 0;
    
    public FlipFlopProxyProvider(Class<T> iface, T activeImpl,
        T standbyImpl) {
      this.iface = iface;
      this.impl1 = activeImpl;
      this.impl2 = standbyImpl;
      currentlyActive = impl1;
    }
    
    @Override
    public ProxyInfo<T> getProxy() {
      return new ProxyInfo<T>(currentlyActive, currentlyActive.toString());
    }

    @Override
    public synchronized void performFailover(Object currentProxy) {
      currentlyActive = impl1 == currentProxy ? impl2 : impl1;
      failoversOccurred++;
    }

    @Override
    public Class<T> getInterface() {
      return iface;
    }

    @Override
    public void close() throws IOException {
      // Nothing to do.
    }
    
    public int getFailoversOccurred() {
      return failoversOccurred;
    }
  }
  
  public static class FailOverOnceOnAnyExceptionPolicy implements RetryPolicy {

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) {
      return failovers < 1 ? RetryAction.FAILOVER_AND_RETRY : RetryAction.FAIL;
    }
    
  }
  
  private static FlipFlopProxyProvider<UnreliableInterface>
      newFlipFlopProxyProvider() {
    return new FlipFlopProxyProvider<UnreliableInterface>(
        UnreliableInterface.class,
        new UnreliableImplementation("impl1"),
        new UnreliableImplementation("impl2"));
  }

  private static FlipFlopProxyProvider<UnreliableInterface>
      newFlipFlopProxyProvider(TypeOfExceptionToFailWith t1,
          TypeOfExceptionToFailWith t2) {
    return new FlipFlopProxyProvider<UnreliableInterface>(
        UnreliableInterface.class,
        new UnreliableImplementation("impl1", t1),
        new UnreliableImplementation("impl2", t2));
  }

  @Test
  public void testSuccedsOnceThenFailOver() throws UnreliableException,
      IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class, newFlipFlopProxyProvider(),
        new FailOverOnceOnAnyExceptionPolicy());
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    assertEquals("impl2", unreliable.succeedsOnceThenFailsReturningString());
    try {
      unreliable.succeedsOnceThenFailsReturningString();
      fail("should not have succeeded more than twice");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testSucceedsTenTimesThenFailOver() throws UnreliableException,
      IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class,
        newFlipFlopProxyProvider(),
        new FailOverOnceOnAnyExceptionPolicy());
    
    for (int i = 0; i < 10; i++) {
      assertEquals("impl1", unreliable.succeedsTenTimesThenFailsReturningString());
    }
    assertEquals("impl2", unreliable.succeedsTenTimesThenFailsReturningString());
  }
  
  @Test
  public void testNeverFailOver() throws UnreliableException,
      IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class,
        newFlipFlopProxyProvider(),
        RetryPolicies.TRY_ONCE_THEN_FAIL);

    unreliable.succeedsOnceThenFailsReturningString();
    try {
      unreliable.succeedsOnceThenFailsReturningString();
      fail("should not have succeeded twice");
    } catch (UnreliableException e) {
      assertEquals("impl1", e.getMessage());
    }
  }
  
  @Test
  public void testFailoverOnStandbyException()
      throws UnreliableException, IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class,
        newFlipFlopProxyProvider(),
        RetryPolicies.failoverOnNetworkException(1));
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    try {
      unreliable.succeedsOnceThenFailsReturningString();
      fail("should not have succeeded twice");
    } catch (UnreliableException e) {
      // Make sure there was no failover on normal exception.
      assertEquals("impl1", e.getMessage());
    }
    
    unreliable = (UnreliableInterface)RetryProxy
    .create(UnreliableInterface.class,
        newFlipFlopProxyProvider(
            TypeOfExceptionToFailWith.STANDBY_EXCEPTION,
            TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION),
        RetryPolicies.failoverOnNetworkException(1));
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    // Make sure we fail over since the first implementation threw a StandbyException
    assertEquals("impl2", unreliable.succeedsOnceThenFailsReturningString());
  }
  
  @Test
  public void testFailoverOnNetworkExceptionIdempotentOperation()
      throws UnreliableException, IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class,
        newFlipFlopProxyProvider(
            TypeOfExceptionToFailWith.IO_EXCEPTION,
            TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION),
        RetryPolicies.failoverOnNetworkException(1));
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    try {
      unreliable.succeedsOnceThenFailsReturningString();
      fail("should not have succeeded twice");
    } catch (IOException e) {
      // Make sure we *don't* fail over since the first implementation threw an
      // IOException and this method is not idempotent
      assertEquals("impl1", e.getMessage());
    }
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningStringIdempotent());
    // Make sure we fail over since the first implementation threw an
    // IOException and this method is idempotent.
    assertEquals("impl2", unreliable.succeedsOnceThenFailsReturningStringIdempotent());
  }
  
  /**
   * Test that if a non-idempotent void function is called, and there is an exception,
   * the exception is properly propagated
   */
  @Test
  public void testExceptionPropagatedForNonIdempotentVoid() throws Exception {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
    .create(UnreliableInterface.class,
        newFlipFlopProxyProvider(
            TypeOfExceptionToFailWith.IO_EXCEPTION,
            TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION),
        RetryPolicies.failoverOnNetworkException(1));

    try {
      unreliable.nonIdempotentVoidFailsIfIdentifierDoesntMatch("impl2");
      fail("did not throw an exception");
    } catch (Exception e) {
    }

  }
  
  private static class SynchronizedUnreliableImplementation extends UnreliableImplementation {
    
    private CountDownLatch methodLatch;
    
    public SynchronizedUnreliableImplementation(String identifier,
        TypeOfExceptionToFailWith exceptionToFailWith, int threadCount) {
      super(identifier, exceptionToFailWith);
      
      methodLatch = new CountDownLatch(threadCount);
    }

    @Override
    public String failsIfIdentifierDoesntMatch(String identifier)
        throws UnreliableException, StandbyException, IOException {
      // Wait until all threads are trying to invoke this method
      methodLatch.countDown();
      try {
        methodLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return super.failsIfIdentifierDoesntMatch(identifier);
    }
    
  }
  
  private static class ConcurrentMethodThread extends Thread {
    
    private UnreliableInterface unreliable;
    public String result;
    
    public ConcurrentMethodThread(UnreliableInterface unreliable) {
      this.unreliable = unreliable;
    }
    
    @Override
    public void run() {
      try {
        result = unreliable.failsIfIdentifierDoesntMatch("impl2");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Test that concurrent failed method invocations only result in a single
   * failover.
   */
  @Test
  public void testConcurrentMethodFailures() throws InterruptedException {
    FlipFlopProxyProvider<UnreliableInterface> proxyProvider
        = new FlipFlopProxyProvider<UnreliableInterface>(
        UnreliableInterface.class,
        new SynchronizedUnreliableImplementation("impl1",
            TypeOfExceptionToFailWith.STANDBY_EXCEPTION,
            2),
        new UnreliableImplementation("impl2",
            TypeOfExceptionToFailWith.STANDBY_EXCEPTION));
    
    final UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
      .create(UnreliableInterface.class, proxyProvider,
          RetryPolicies.failoverOnNetworkException(10));

    ConcurrentMethodThread t1 = new ConcurrentMethodThread(unreliable);
    ConcurrentMethodThread t2 = new ConcurrentMethodThread(unreliable);
    
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    assertEquals("impl2", t1.result);
    assertEquals("impl2", t2.result);
    assertEquals(1, proxyProvider.getFailoversOccurred());
  }

  /**
   * Ensure that when all configured services are throwing StandbyException
   * that we fail over back and forth between them until one is no longer
   * throwing StandbyException.
   */
  @Test
  public void testFailoverBetweenMultipleStandbys()
      throws UnreliableException, StandbyException, IOException {
    
    final long millisToSleep = 10000;
    
    final UnreliableImplementation impl1 = new UnreliableImplementation("impl1",
        TypeOfExceptionToFailWith.STANDBY_EXCEPTION);
    FlipFlopProxyProvider<UnreliableInterface> proxyProvider
        = new FlipFlopProxyProvider<UnreliableInterface>(
        UnreliableInterface.class,
        impl1,
        new UnreliableImplementation("impl2",
            TypeOfExceptionToFailWith.STANDBY_EXCEPTION));
    
    final UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
      .create(UnreliableInterface.class, proxyProvider,
          RetryPolicies.failoverOnNetworkException(
              RetryPolicies.TRY_ONCE_THEN_FAIL, 10, 1000, 10000));
    
    new Thread() {
      @Override
      public void run() {
        ThreadUtil.sleepAtLeastIgnoreInterrupts(millisToSleep);
        impl1.setIdentifier("renamed-impl1");
      }
    }.start();
    
    String result = unreliable.failsIfIdentifierDoesntMatch("renamed-impl1");
    assertEquals("renamed-impl1", result);
  }
  
  /**
   * Ensure that normal IO exceptions don't result in a failover.
   */
  @Test
  public void testExpectedIOException() {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.create(
        UnreliableInterface.class,
        newFlipFlopProxyProvider(
            TypeOfExceptionToFailWith.REMOTE_EXCEPTION,
            TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION),
        RetryPolicies.failoverOnNetworkException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, 10, 1000, 10000));
    
    try {
      unreliable.failsIfIdentifierDoesntMatch("no-such-identifier");
      fail("Should have thrown *some* exception");
    } catch (Exception e) {
      assertTrue("Expected IOE but got " + e.getClass(),
          e instanceof IOException);
    }
  }
}