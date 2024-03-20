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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;

public class AbfsManagedHttpRequestExecutor extends HttpRequestExecutor {

  public AbfsManagedHttpRequestExecutor(final int expect100WaitTimeout) {
    super(expect100WaitTimeout);
  }

  @Override
  protected HttpResponse doSendRequest(final HttpRequest request,
      final HttpClientConnection conn,
      final HttpContext context) throws IOException, HttpException {
    final HttpClientConnection inteceptedConnection;
    if (context instanceof AbfsManagedHttpContext) {
      inteceptedConnection
          = ((AbfsManagedHttpContext) context).interceptConnectionActivity(
          conn);
    } else {
      inteceptedConnection = conn;
    }
//      long start = System.currentTimeMillis();
    final HttpResponse res = super.doSendRequest(request, inteceptedConnection,
        context);
//      long elapsed = System.currentTimeMillis() - start;
    if (context instanceof AbfsManagedHttpContext) {
      ((AbfsManagedHttpContext) context).httpClientConnection = conn;
//        ((AbfsHttpClientContext) context).sendTime = elapsed;
    }
    if (request != null && request.containsHeader(EXPECT) && res != null
        && res.getStatusLine().getStatusCode() != 200) {
      throw new AbfsApacheHttpExpect100Exception(EXPECT_100_JDK_ERROR, res);
    }
    return res;
  }

  @Override
  protected HttpResponse doReceiveResponse(final HttpRequest request,
      final HttpClientConnection conn,
      final HttpContext context) throws HttpException, IOException {
    final HttpClientConnection interceptedConnection;
    if (context instanceof AbfsManagedHttpContext) {
      interceptedConnection
          = ((AbfsManagedHttpContext) context).interceptConnectionActivity(
          conn);
    } else {
      interceptedConnection = conn;
    }
    long start = System.currentTimeMillis();
    final HttpResponse res = super.doReceiveResponse(request,
        interceptedConnection, context);
    long elapsed = System.currentTimeMillis() - start;
    if (context instanceof AbfsManagedHttpContext) {
      ((AbfsManagedHttpContext) context).readTime = elapsed;
    }
    return res;
  }
}
