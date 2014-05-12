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

package org.apache.hadoop.yarn.server.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.junit.Test;

public class TestLeveldbIterator {
  private static class CallInfo {
    String methodName;
    Object[] args;
    Class<?>[] argTypes;

    public CallInfo(String methodName, Object... args) {
      this.methodName = methodName;
      this.args = args;
      argTypes = new Class[args.length];
      for (int i = 0; i < args.length; ++i) {
        argTypes[i] = args[i].getClass();
      }
    }
  }

  // array of methods that should throw DBException instead of raw
  // runtime exceptions
  private static CallInfo[] RTEXC_METHODS = new CallInfo[] {
    new CallInfo("seek", new byte[0]),
    new CallInfo("seekToFirst"),
    new CallInfo("seekToLast"),
    new CallInfo("hasNext"),
    new CallInfo("next"),
    new CallInfo("peekNext"),
    new CallInfo("hasPrev"),
    new CallInfo("prev"),
    new CallInfo("peekPrev"),
    new CallInfo("remove")
  };

  @Test
  public void testExceptionHandling() throws Exception {
    InvocationHandler rtExcHandler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
          throws Throwable {
        throw new RuntimeException("forced runtime error");
      }
    };
    DBIterator dbiter = (DBIterator) Proxy.newProxyInstance(
        DBIterator.class.getClassLoader(), new Class[] { DBIterator.class },
        rtExcHandler);
    LeveldbIterator iter = new LeveldbIterator(dbiter);
    for (CallInfo ci : RTEXC_METHODS) {
      Method method = iter.getClass().getMethod(ci.methodName, ci.argTypes);
      assertNotNull("unable to locate method " + ci.methodName, method);
      try {
        method.invoke(iter, ci.args);
        fail("operation should have thrown");
      } catch (InvocationTargetException ite) {
        Throwable exc = ite.getTargetException();
        assertTrue("Method " + ci.methodName + " threw non-DBException: "
            + exc, exc instanceof DBException);
        assertFalse("Method " + ci.methodName + " double-wrapped DBException",
            exc.getCause() instanceof DBException);
      }
    }

    // check close() throws IOException
    try {
      iter.close();
      fail("operation shoul have thrown");
    } catch (IOException e) {
      // expected
    }
  }
}
