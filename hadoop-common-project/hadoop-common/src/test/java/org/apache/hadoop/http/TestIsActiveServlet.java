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
package org.apache.hadoop.http;


import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test if the {@link IsActiveServlet} returns the right answer if the
 * underlying service is active.
 */
public class TestIsActiveServlet {

  private IsActiveServlet servlet;
  private HttpServletRequest req;
  private HttpServletResponse resp;
  private ByteArrayOutputStream respOut;

  @Before
  public void setUp() throws Exception {
    req = mock(HttpServletRequest.class);
    resp = mock(HttpServletResponse.class);
    respOut = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(respOut);
    when(resp.getWriter()).thenReturn(writer);
  }

  @Test
  public void testSucceedsOnActive() throws IOException {
    servlet = new IsActiveServlet() {
      @Override
      protected boolean isActive() {
        return true;
      }
    };

    String response = doGet();
    verify(resp, never()).sendError(anyInt(), anyString());
    assertEquals(IsActiveServlet.RESPONSE_ACTIVE, response);
  }

  @Test
  public void testFailsOnInactive() throws IOException {
    servlet = new IsActiveServlet() {
      @Override
      protected boolean isActive() {
        return false;
      }
    };

    doGet();
    verify(resp, atLeastOnce()).sendError(
        eq(HttpServletResponse.SC_METHOD_NOT_ALLOWED),
        eq(IsActiveServlet.RESPONSE_NOT_ACTIVE));
  }

  private String doGet() throws IOException {
    servlet.doGet(req, resp);
    return new String(respOut.toByteArray(), "UTF-8");
  }
}
