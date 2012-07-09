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
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;

/**
 * Test provides some very generic helpers which might be used across the tests
 */
public abstract class GenericTestUtils {

  /**
   * Extracts the name of the method where the invocation has happened
   * @return String name of the invoking method
   */
  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
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
        Joiner.on(",").join(found),
        Joiner.on(",").join(expectedSet));
  }
  
  public static void assertExceptionContains(String string, Throwable t) {
    String msg = t.getMessage();
    Assert.assertTrue(
        "Unexpected exception:" + StringUtils.stringifyException(t),
        msg.contains(string));    
  }  

  public static void waitFor(Supplier<Boolean> check,
      int checkEveryMillis, int waitForMillis)
      throws TimeoutException, InterruptedException
  {
    long st = System.currentTimeMillis();
    do {
      boolean result = check.get();
      if (result) {
        return;
      }
      
      Thread.sleep(checkEveryMillis);
    } while (System.currentTimeMillis() - st < waitForMillis);
    throw new TimeoutException("Timed out waiting for condition");
  }
  
  
  /**
   * Mockito answer helper that triggers one latch as soon as the
   * method is called, then waits on another before continuing.
   */
  public static class DelayAnswer implements Answer<Object> {
    private final Log LOG;
    
    private final CountDownLatch fireLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
  
    
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
  
    public Object answer(InvocationOnMock invocation) throws Throwable {
      LOG.info("DelayAnswer firing fireLatch");
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
      return invocation.callRealMethod();
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
    
    public DelegateAnswer(Object delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      return invocation.getMethod().invoke(
          delegate, invocation.getArguments());
    }
  }

}
