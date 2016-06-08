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

package org.apache.hadoop.io.retry;

import static org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER;
import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByRemoteException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryOtherThanRemoteException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithProportionalSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumTimeWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.retry.UnreliableInterface.FatalException;
import org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.UndeclaredThrowableException;

public class TestRetryProxy {
  
  private UnreliableImplementation unreliableImpl;
  
  @Before
  public void setUp() throws Exception {
    unreliableImpl = new UnreliableImplementation();
  }

  @Test
  public void testTryOnceThenFail() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, TRY_ONCE_THEN_FAIL);
    unreliable.alwaysSucceeds();
    try {
      unreliable.failsOnceThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  /**
   * Test for {@link RetryInvocationHandler#isRpcInvocation(Object)}
   */
  @Test
  public void testRpcInvocation() throws Exception {
    // For a proxy method should return true
    final UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    assertTrue(RetryInvocationHandler.isRpcInvocation(unreliable));
    
    // Embed the proxy in ProtocolTranslator
    ProtocolTranslator xlator = new ProtocolTranslator() {
      int count = 0;
      @Override
      public Object getUnderlyingProxyObject() {
        count++;
        return unreliable;
      }
      @Override
      public String toString() {
        return "" + count;
      }
    };
    
    // For a proxy wrapped in ProtocolTranslator method should return true
    assertTrue(RetryInvocationHandler.isRpcInvocation(xlator));
    // Ensure underlying proxy was looked at
    assertEquals(xlator.toString(), "1");
    
    // For non-proxy the method must return false
    assertFalse(RetryInvocationHandler.isRpcInvocation(new Object()));
  }
  
  @Test
  public void testRetryForever() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    unreliable.failsTenTimesThenSucceeds();
  }
  
  @Test
  public void testRetryUpToMaximumCountWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumCountWithFixedSleep(8, 1, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testRetryUpToMaximumTimeWithFixedSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumTimeWithFixedSleep(80, 10, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testRetryUpToMaximumCountWithProportionalSleep() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryUpToMaximumCountWithProportionalSleep(8, 1, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testExponentialRetry() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        exponentialBackoffRetry(5, 1L, TimeUnit.NANOSECONDS));
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsTenTimesThenSucceeds();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  @Test
  public void testRetryByException() throws UnreliableException {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(FatalException.class, TRY_ONCE_THEN_FAIL);
    
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryByException(RETRY_FOREVER, exceptionToPolicyMap));
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.alwaysFailsWithFatalException();
      fail("Should fail");
    } catch (FatalException e) {
      // expected
    }
  }
  
  @Test
  public void testRetryByRemoteException() {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(FatalException.class, TRY_ONCE_THEN_FAIL);
    
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryByRemoteException(RETRY_FOREVER, exceptionToPolicyMap));
    try {
      unreliable.alwaysFailsWithRemoteFatalException();
      fail("Should fail");
    } catch (RemoteException e) {
      // expected
    }
  }

  @Test
  public void testRetryOtherThanRemoteException() throws Throwable {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(
            IOException.class, RETRY_FOREVER);

    UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl,
            retryOtherThanRemoteException(TRY_ONCE_THEN_FAIL,
                exceptionToPolicyMap));
    // should retry with local IOException.
    unreliable.failsOnceWithIOException();
    try {
      // won't get retry on remote exception
      unreliable.failsOnceWithRemoteException();
      fail("Should fail");
    } catch (RemoteException e) {
      // expected
    }
  }

  @Test
  public void testRetryInterruptible() throws Throwable {
    final UnreliableInterface unreliable = (UnreliableInterface)
        RetryProxy.create(UnreliableInterface.class, unreliableImpl,
            retryUpToMaximumTimeWithFixedSleep(10, 10, TimeUnit.SECONDS));
    
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Thread> futureThread = new AtomicReference<Thread>();
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<Throwable> future = exec.submit(new Callable<Throwable>(){
      @Override
      public Throwable call() throws Exception {
        futureThread.set(Thread.currentThread());
        latch.countDown();
        try {
          unreliable.alwaysFailsWithFatalException();
        } catch (UndeclaredThrowableException ute) {
          return ute.getCause();
        }
        return null;
      }
    });
    latch.await();
    Thread.sleep(1000); // time to fail and sleep
    assertTrue(futureThread.get().isAlive());
    futureThread.get().interrupt();
    Throwable e = future.get(1, TimeUnit.SECONDS); // should return immediately 
    assertNotNull(e);
    assertEquals(InterruptedException.class, e.getClass());
    assertEquals("sleep interrupted", e.getMessage());
  }
}
