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

import org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith;
import org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException;
import org.apache.hadoop.ipc.StandbyException;
import org.junit.Test;

public class TestFailoverProxy {

  public static class FlipFlopProxyProvider implements FailoverProxyProvider {
    
    private Class<?> iface;
    private Object currentlyActive;
    private Object impl1;
    private Object impl2;
    
    public FlipFlopProxyProvider(Class<?> iface, Object activeImpl,
        Object standbyImpl) {
      this.iface = iface;
      this.impl1 = activeImpl;
      this.impl2 = standbyImpl;
      currentlyActive = impl1;
    }
    
    @Override
    public Object getProxy() {
      return currentlyActive;
    }

    @Override
    public void performFailover(Object currentProxy) {
      currentlyActive = impl1 == currentProxy ? impl2 : impl1;
    }

    @Override
    public Class<?> getInterface() {
      return iface;
    }

    @Override
    public void close() throws IOException {
      // Nothing to do.
    }
    
  }
  
  public static class FailOverOnceOnAnyExceptionPolicy implements RetryPolicy {

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) {
      return failovers < 1 ? RetryAction.FAILOVER_AND_RETRY : RetryAction.FAIL;
    }
    
  }
  
  @Test
  public void testSuccedsOnceThenFailOver() throws UnreliableException,
      IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
        .create(UnreliableInterface.class,
            new FlipFlopProxyProvider(UnreliableInterface.class,
              new UnreliableImplementation("impl1"),
              new UnreliableImplementation("impl2")),
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
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
        .create(UnreliableInterface.class,
            new FlipFlopProxyProvider(UnreliableInterface.class,
              new UnreliableImplementation("impl1"),
              new UnreliableImplementation("impl2")),
            new FailOverOnceOnAnyExceptionPolicy());
    
    for (int i = 0; i < 10; i++) {
      assertEquals("impl1", unreliable.succeedsTenTimesThenFailsReturningString());
    }
    assertEquals("impl2", unreliable.succeedsTenTimesThenFailsReturningString());
  }
  
  @Test
  public void testNeverFailOver() throws UnreliableException,
      IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
    .create(UnreliableInterface.class,
        new FlipFlopProxyProvider(UnreliableInterface.class,
          new UnreliableImplementation("impl1"),
          new UnreliableImplementation("impl2")),
        RetryPolicies.TRY_ONCE_DONT_FAIL);

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
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
    .create(UnreliableInterface.class,
        new FlipFlopProxyProvider(UnreliableInterface.class,
          new UnreliableImplementation("impl1"),
          new UnreliableImplementation("impl2")),
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
        new FlipFlopProxyProvider(UnreliableInterface.class,
          new UnreliableImplementation("impl1", TypeOfExceptionToFailWith.STANDBY_EXCEPTION),
          new UnreliableImplementation("impl2", TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION)),
        RetryPolicies.failoverOnNetworkException(1));
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    // Make sure we fail over since the first implementation threw a StandbyException
    assertEquals("impl2", unreliable.succeedsOnceThenFailsReturningString());
  }
  
  @Test
  public void testFailoverOnNetworkExceptionIdempotentOperation()
      throws UnreliableException, IOException, StandbyException {
    UnreliableInterface unreliable = (UnreliableInterface)RetryProxy
    .create(UnreliableInterface.class,
        new FlipFlopProxyProvider(UnreliableInterface.class,
          new UnreliableImplementation("impl1", TypeOfExceptionToFailWith.IO_EXCEPTION),
          new UnreliableImplementation("impl2", TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION)),
        RetryPolicies.failoverOnNetworkException(1));
    
    assertEquals("impl1", unreliable.succeedsOnceThenFailsReturningString());
    try {
      assertEquals("impl2", unreliable.succeedsOnceThenFailsReturningString());
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
}
