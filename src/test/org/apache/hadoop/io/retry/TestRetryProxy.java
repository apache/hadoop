package org.apache.hadoop.io.retry;

import static org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER;
import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_DONT_FAIL;
import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
import static org.apache.hadoop.io.retry.RetryPolicies.retryByException;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithProportionalSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumTimeWithFixedSleep;
import static org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.io.retry.UnreliableInterface.FatalException;
import org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException;

public class TestRetryProxy extends TestCase {
  
  private UnreliableImplementation unreliableImpl;
  
  @Override
  protected void setUp() throws Exception {
    unreliableImpl = new UnreliableImplementation();
  }

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
  
  public void testTryOnceDontFail() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, TRY_ONCE_DONT_FAIL);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.failsOnceThenSucceedsWithReturnValue();
      fail("Should fail");
    } catch (UnreliableException e) {
      // expected
    }
  }
  
  public void testRetryForever() throws UnreliableException {
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl, RETRY_FOREVER);
    unreliable.alwaysSucceeds();
    unreliable.failsOnceThenSucceeds();
    unreliable.failsTenTimesThenSucceeds();
  }
  
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
  
  public void testRetryByException() throws UnreliableException {
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      Collections.<Class<? extends Exception>, RetryPolicy>singletonMap(FatalException.class, TRY_ONCE_THEN_FAIL);
    
    UnreliableInterface unreliable = (UnreliableInterface)
      RetryProxy.create(UnreliableInterface.class, unreliableImpl,
                        retryByException(RETRY_FOREVER, exceptionToPolicyMap));
    unreliable.failsOnceThenSucceeds();
    try {
      unreliable.alwaysfailsWithFatalException();
      fail("Should fail");
    } catch (FatalException e) {
      // expected
    }
  }
  
}
