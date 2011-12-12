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

import junit.framework.Assert;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestHTestCase extends HTestCase {

  @Test(expected = IllegalStateException.class)
  public void testDirNoAnnotation() throws Exception {
    TestDirHelper.getTestDir();
  }

  @Test(expected = IllegalStateException.class)
  public void testJettyNoAnnotation() throws Exception {
    TestJettyHelper.getJettyServer();
  }

  @Test(expected = IllegalStateException.class)
  public void testJettyNoAnnotation2() throws Exception {
    TestJettyHelper.getJettyURL();
  }

  @Test
  @TestDir
  public void testDirAnnotation() throws Exception {
    Assert.assertNotNull(TestDirHelper.getTestDir());
  }

  @Test
  public void waitFor() {
    long start = System.currentTimeMillis();
    long waited = waitFor(1000, new Predicate() {
      public boolean evaluate() throws Exception {
        return true;
      }
    });
    long end = System.currentTimeMillis();
    Assert.assertEquals(waited, 0, 50);
    Assert.assertEquals(end - start - waited, 0, 50);
  }

  @Test
  public void waitForTimeOutRatio1() {
    setWaitForRatio(1);
    long start = System.currentTimeMillis();
    long waited = waitFor(200, new Predicate() {
      public boolean evaluate() throws Exception {
        return false;
      }
    });
    long end = System.currentTimeMillis();
    Assert.assertEquals(waited, -1);
    Assert.assertEquals(end - start, 200, 50);
  }

  @Test
  public void waitForTimeOutRatio2() {
    setWaitForRatio(2);
    long start = System.currentTimeMillis();
    long waited = waitFor(200, new Predicate() {
      public boolean evaluate() throws Exception {
        return false;
      }
    });
    long end = System.currentTimeMillis();
    Assert.assertEquals(waited, -1);
    Assert.assertEquals(end - start, 200 * getWaitForRatio(), 50 * getWaitForRatio());
  }

  @Test
  public void sleepRatio1() {
    setWaitForRatio(1);
    long start = System.currentTimeMillis();
    sleep(100);
    long end = System.currentTimeMillis();
    Assert.assertEquals(end - start, 100, 50);
  }

  @Test
  public void sleepRatio2() {
    setWaitForRatio(1);
    long start = System.currentTimeMillis();
    sleep(100);
    long end = System.currentTimeMillis();
    Assert.assertEquals(end - start, 100 * getWaitForRatio(), 50 * getWaitForRatio());
  }

  public static class MyServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.getWriter().write("foo");
    }
  }

  @Test
  @TestJetty
  public void testJetty() throws Exception {
    Context context = new Context();
    context.setContextPath("/");
    context.addServlet(MyServlet.class, "/bar");
    Server server = TestJettyHelper.getJettyServer();
    server.addHandler(context);
    server.start();
    URL url = new URL(TestJettyHelper.getJettyURL(), "/bar");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    Assert.assertEquals(reader.readLine(), "foo");
    reader.close();
  }

  @Test
  @TestException(exception = RuntimeException.class)
  public void testException0() {
    throw new RuntimeException("foo");
  }

  @Test
  @TestException(exception = RuntimeException.class, msgRegExp = ".o.")
  public void testException1() {
    throw new RuntimeException("foo");
  }

}
