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

package org.apache.hadoop.lib.lang;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;

import org.apache.hadoop.test.HTestCase;
import org.junit.Test;

public class TestRunnableCallable extends HTestCase {

  public static class R implements Runnable {
    boolean RUN;

    @Override
    public void run() {
      RUN = true;
    }
  }

  public static class C implements Callable {
    boolean RUN;

    @Override
    public Object call() throws Exception {
      RUN = true;
      return null;
    }
  }

  public static class CEx implements Callable {

    @Override
    public Object call() throws Exception {
      throw new Exception();
    }
  }

  @Test
  public void runnable() throws Exception {
    R r = new R();
    RunnableCallable rc = new RunnableCallable(r);
    rc.run();
    assertTrue(r.RUN);

    r = new R();
    rc = new RunnableCallable(r);
    rc.call();
    assertTrue(r.RUN);

    assertEquals(rc.toString(), "R");
  }

  @Test
  public void callable() throws Exception {
    C c = new C();
    RunnableCallable rc = new RunnableCallable(c);
    rc.run();
    assertTrue(c.RUN);

    c = new C();
    rc = new RunnableCallable(c);
    rc.call();
    assertTrue(c.RUN);

    assertEquals(rc.toString(), "C");
  }

  @Test(expected = RuntimeException.class)
  public void callableExRun() throws Exception {
    CEx c = new CEx();
    RunnableCallable rc = new RunnableCallable(c);
    rc.run();
  }

}
