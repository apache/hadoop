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

package org.apache.hadoop.fs.http.server;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCheckUploadContentTypeFilter {

  @Test
  public void putUpload() throws Exception {
    test("PUT", HttpFSFileSystem.Operation.CREATE.toString(), "application/octet-stream", true, false);
  }

  @Test
  public void postUpload() throws Exception {
    test("POST", HttpFSFileSystem.Operation.APPEND.toString(), "APPLICATION/OCTET-STREAM", true, false);
  }

  @Test
  public void putUploadWrong() throws Exception {
    test("PUT", HttpFSFileSystem.Operation.CREATE.toString(), "plain/text", false, false);
    test("PUT", HttpFSFileSystem.Operation.CREATE.toString(), "plain/text", true, true);
  }

  @Test
  public void postUploadWrong() throws Exception {
    test("POST", HttpFSFileSystem.Operation.APPEND.toString(), "plain/text", false, false);
    test("POST", HttpFSFileSystem.Operation.APPEND.toString(), "plain/text", true, true);
  }

  @Test
  public void getOther() throws Exception {
    test("GET", HttpFSFileSystem.Operation.GETHOMEDIRECTORY.toString(), "plain/text", false, false);
  }

  @Test
  public void putOther() throws Exception {
    test("PUT", HttpFSFileSystem.Operation.MKDIRS.toString(), "plain/text", false, false);
  }

  private void test(String method, String operation, String contentType,
                    boolean upload, boolean error) throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.reset(request);
    Mockito.when(request.getMethod()).thenReturn(method);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).thenReturn(operation);
    Mockito.when(request.getParameter(HttpFSParametersProvider.DataParam.NAME)).
      thenReturn(Boolean.toString(upload));
    Mockito.when(request.getContentType()).thenReturn(contentType);

    FilterChain chain = Mockito.mock(FilterChain.class);

    Filter filter = new CheckUploadContentTypeFilter();

    filter.doFilter(request, response, chain);

    if (error) {
      Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
                                         Mockito.contains("Data upload"));
    }
    else {
      Mockito.verify(chain).doFilter(request, response);
    }
  }


}
