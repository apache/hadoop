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

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;

/**
 * This class extends {@link HttpRequestExecutor} to intercept the connection
 * activity and register the latency of different phases of a network call. It
 * also overrides the HttpRequestExecutor's expect100 failure handling as the ADLS
 * can send any failure statusCode in expect100 hand-shake failure and non
 * necessarily 1XX code.
 */
public class AbfsManagedHttpRequestExecutor extends HttpRequestExecutor {

  public AbfsManagedHttpRequestExecutor(final int expect100WaitTimeout) {
    super(expect100WaitTimeout);
  }

  /**{@inheritDoc}*/
  @Override
  public HttpResponse execute(final HttpRequest request,
      final HttpClientConnection conn,
      final HttpContext context) throws IOException, HttpException {
    if (context instanceof AbfsManagedHttpClientContext
        && conn instanceof AbfsManagedApacheHttpConnection) {
      ((AbfsManagedApacheHttpConnection) conn).setManagedHttpContext(
          (AbfsManagedHttpClientContext) context);
    }
    return super.execute(request, conn, context);
  }

  /**{@inheritDoc}*/
  @Override
  protected HttpResponse doSendRequest(final HttpRequest request,
      final HttpClientConnection conn,
      final HttpContext context) throws IOException, HttpException {
    final HttpClientConnection inteceptedConnection;
    if (context instanceof AbfsManagedHttpClientContext) {
      inteceptedConnection
          = ((AbfsManagedHttpClientContext) context).interceptConnectionActivity(
          conn);
    } else {
      inteceptedConnection = conn;
    }
    final HttpResponse res = super.doSendRequest(request, inteceptedConnection,
        context);

    /*
     * ApacheHttpClient implementation does not raise an exception if the status
     * of expect100 hand-shake is not less than 200. Although it sends payload only
     * if the statusCode of the expect100 hand-shake is 100.
     *
     * ADLS can send any failure statusCode in exect100 handshake. So, an exception
     * needs to be explicitly raised if expect100 assertion is failure but the
     * ApacheHttpClient has not raised an exception.
     *
     * Response is only returned by this method if there is no expect100 request header
     * or the expect100 assertion is failed.
     */
    if (request != null && request.containsHeader(EXPECT) && res != null) {
      throw new AbfsApacheHttpExpect100Exception(res);
    }
    return res;
  }

  /**{@inheritDoc}*/
  @Override
  protected HttpResponse doReceiveResponse(final HttpRequest request,
      final HttpClientConnection conn,
      final HttpContext context) throws HttpException, IOException {
    final HttpClientConnection interceptedConnection;
    if (context instanceof AbfsManagedHttpClientContext) {
      interceptedConnection
          = ((AbfsManagedHttpClientContext) context).interceptConnectionActivity(
          conn);
    } else {
      interceptedConnection = conn;
    }
    return super.doReceiveResponse(request,
        interceptedConnection, context);
  }
}
