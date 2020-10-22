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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressTestHelper.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;

public class TestStartupProgressServlet {

  private HttpServletRequest req;
  private HttpServletResponse resp;
  private ByteArrayOutputStream respOut;
  private StartupProgress startupProgress;
  private StartupProgressServlet servlet;

  @Before
  public void setUp() throws Exception {
    startupProgress = new StartupProgress();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(NameNodeHttpServer.STARTUP_PROGRESS_ATTRIBUTE_KEY))
      .thenReturn(startupProgress);
    servlet = mock(StartupProgressServlet.class);
    when(servlet.getServletContext()).thenReturn(context);
    doCallRealMethod().when(servlet).doGet(any(HttpServletRequest.class),
      any(HttpServletResponse.class));
    req = mock(HttpServletRequest.class);
    respOut = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(respOut);
    resp = mock(HttpServletResponse.class);
    when(resp.getWriter()).thenReturn(writer);
  }

  @Test
  public void testInitialState() throws Exception {
    String respBody = doGetAndReturnResponseBody();
    assertNotNull(respBody);

    Map<String, Object> expected = ImmutableMap.<String, Object>builder()
      .put("percentComplete", 0.0f)
      .put("phases", Arrays.<Object>asList(
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingFsImage")
          .put("desc", "Loading fsimage")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingEdits")
          .put("desc", "Loading edits")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SavingCheckpoint")
          .put("desc", "Saving checkpoint")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SafeMode")
          .put("desc", "Safe mode")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build()))
      .build();

    assertEquals(JSON.toString(expected), filterJson(respBody));
  }

  @Test
  public void testRunningState() throws Exception {
    setStartupProgressForRunningState(startupProgress);
    String respBody = doGetAndReturnResponseBody();
    assertNotNull(respBody);

    Map<String, Object> expected = ImmutableMap.<String, Object>builder()
      .put("percentComplete", 0.375f)
      .put("phases", Arrays.<Object>asList(
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingFsImage")
          .put("desc", "Loading fsimage")
          .put("status", "COMPLETE")
          .put("percentComplete", 1.0f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("name", "Inodes")
              .put("desc", "inodes")
              .put("count", 100L)
              .put("total", 100L)
              .put("percentComplete", 1.0f)
              .build()
          ))
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingEdits")
          .put("desc", "Loading edits")
          .put("status", "RUNNING")
          .put("percentComplete", 0.5f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("count", 100L)
              .put("file", "file")
              .put("size", 1000L)
              .put("total", 200L)
            .put("percentComplete", 0.5f)
              .build()
          ))
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SavingCheckpoint")
          .put("desc", "Saving checkpoint")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SafeMode")
          .put("desc", "Safe mode")
          .put("status", "PENDING")
          .put("percentComplete", 0.0f)
          .put("steps", Collections.emptyList())
          .build()))
      .build();

    assertEquals(JSON.toString(expected), filterJson(respBody));
  }

  @Test
  public void testFinalState() throws Exception {
    setStartupProgressForFinalState(startupProgress);
    String respBody = doGetAndReturnResponseBody();
    assertNotNull(respBody);

    Map<String, Object> expected = ImmutableMap.<String, Object>builder()
      .put("percentComplete", 1.0f)
      .put("phases", Arrays.<Object>asList(
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingFsImage")
          .put("desc", "Loading fsimage")
          .put("status", "COMPLETE")
          .put("percentComplete", 1.0f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("name", "Inodes")
              .put("desc", "inodes")
              .put("count", 100L)
              .put("total", 100L)
              .put("percentComplete", 1.0f)
              .build()
          ))
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "LoadingEdits")
          .put("desc", "Loading edits")
          .put("status", "COMPLETE")
          .put("percentComplete", 1.0f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("count", 200L)
              .put("file", "file")
              .put("size", 1000L)
              .put("total", 200L)
              .put("percentComplete", 1.0f)
              .build()
          ))
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SavingCheckpoint")
          .put("desc", "Saving checkpoint")
          .put("status", "COMPLETE")
          .put("percentComplete", 1.0f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("name", "Inodes")
              .put("desc", "inodes")
              .put("count", 300L)
              .put("total", 300L)
              .put("percentComplete", 1.0f)
              .build()
          ))
          .build(),
        ImmutableMap.<String, Object>builder()
          .put("name", "SafeMode")
          .put("desc", "Safe mode")
          .put("status", "COMPLETE")
          .put("percentComplete", 1.0f)
          .put("steps", Collections.<Object>singletonList(
            ImmutableMap.<String, Object>builder()
              .put("name", "AwaitingReportedBlocks")
              .put("desc", "awaiting reported blocks")
              .put("count", 400L)
              .put("total", 400L)
              .put("percentComplete", 1.0f)
              .build()
          ))
          .build()))
      .build();

    assertEquals(JSON.toString(expected), filterJson(respBody));
  }

  /**
   * Calls doGet on the servlet, captures the response body as a string, and
   * returns it to the caller.
   * 
   * @return String response body
   * @throws IOException thrown if there is an I/O error
   */
  private String doGetAndReturnResponseBody() throws IOException {
    servlet.doGet(req, resp);
    return new String(respOut.toByteArray(), "UTF-8");
  }

  /**
   * Filters the given JSON response body, removing elements that would impede
   * testing.  Specifically, it removes elapsedTime fields, because we cannot
   * predict the exact values.
   * 
   * @param str String to filter
   * @return String filtered value
   */
  private String filterJson(String str) {
    return str.replaceAll("\"elapsedTime\":\\d+\\,", "")
      .replaceAll("\\,\"elapsedTime\":\\d+", "");
  }
}
