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

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;

import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;

/**
 * This class wraps the {@link ManagedHttpClientConnection} and provides
 * insights onto the connection level activity.
 */
class AbfsManagedApacheHttpConnection
    implements ManagedHttpClientConnection {

  /**
   * Underlying ApacheHttpClient connection that actually does the work over network.
   */
  private final ManagedHttpClientConnection httpClientConnection;

  /**
   * Managed HTTP context to track the connection level activity.
   */
  private AbfsManagedHttpClientContext managedHttpContext;

  private final HttpHost targetHost;

  private final int hashCode;

  AbfsManagedApacheHttpConnection(ManagedHttpClientConnection conn,
      final HttpRoute route) {
    if (route != null) {
      targetHost = route.getTargetHost();
    } else {
      targetHost = null;
    }
    this.httpClientConnection = conn;
    this.hashCode = (UUID.randomUUID().toString()
        + httpClientConnection.getId()).hashCode();
  }

  /**
   * Sets the managed HTTP context to track the connection level activity.
   */
  void setManagedHttpContext(AbfsManagedHttpClientContext managedHttpContext) {
    this.managedHttpContext = managedHttpContext;
  }

  /**{@inheritDoc}*/
  @Override
  public void close() throws IOException {
    httpClientConnection.close();
  }

  /**{@inheritDoc}*/

  @Override
  public boolean isOpen() {
    return httpClientConnection.isOpen();
  }

  /**{@inheritDoc}*/
  @Override
  public boolean isStale() {
    return httpClientConnection.isStale();
  }

  /**{@inheritDoc}*/
  @Override
  public void setSocketTimeout(final int timeout) {
    httpClientConnection.setSocketTimeout(timeout);
  }

  /**{@inheritDoc}*/
  @Override
  public int getSocketTimeout() {
    return httpClientConnection.getSocketTimeout();
  }

  /**{@inheritDoc}*/
  @Override
  public void shutdown() throws IOException {
    httpClientConnection.shutdown();
  }

  /**{@inheritDoc}*/
  @Override
  public HttpConnectionMetrics getMetrics() {
    return httpClientConnection.getMetrics();
  }

  /**{@inheritDoc}*/
  @Override
  public boolean isResponseAvailable(final int timeout) throws IOException {
    long start = System.currentTimeMillis();
    boolean val = httpClientConnection.isResponseAvailable(timeout);
    managedHttpContext.addReadTime(System.currentTimeMillis() - start);
    return val;
  }

  /**{@inheritDoc}*/
  @Override
  public void sendRequestHeader(final HttpRequest request)
      throws HttpException, IOException {
    long start = System.currentTimeMillis();
    httpClientConnection.sendRequestHeader(request);
    managedHttpContext.addSendTime(System.currentTimeMillis() - start);
  }

  /**{@inheritDoc}*/
  @Override
  public void sendRequestEntity(final HttpEntityEnclosingRequest request)
      throws HttpException, IOException {
    long start = System.currentTimeMillis();
    httpClientConnection.sendRequestEntity(request);
    managedHttpContext.addSendTime(System.currentTimeMillis() - start);
  }

  /**{@inheritDoc}*/
  @Override
  public HttpResponse receiveResponseHeader()
      throws HttpException, IOException {
    long start = System.currentTimeMillis();
    HttpResponse response = httpClientConnection.receiveResponseHeader();
    managedHttpContext.addReadTime(System.currentTimeMillis() - start);
    return response;
  }

  /**{@inheritDoc}*/
  @Override
  public void receiveResponseEntity(final HttpResponse response)
      throws HttpException, IOException {
    long start = System.currentTimeMillis();
    httpClientConnection.receiveResponseEntity(response);
    managedHttpContext.addReadTime(System.currentTimeMillis() - start);
  }

  /**{@inheritDoc}*/
  @Override
  public void flush() throws IOException {
    long start = System.currentTimeMillis();
    httpClientConnection.flush();
    managedHttpContext.addSendTime(System.currentTimeMillis() - start);
  }

  /**{@inheritDoc}*/
  @Override
  public String getId() {
    return httpClientConnection.getId();
  }

  /**{@inheritDoc}*/
  @Override
  public void bind(final Socket socket) throws IOException {
    httpClientConnection.bind(socket);
  }

  /**{@inheritDoc}*/
  @Override
  public Socket getSocket() {
    return httpClientConnection.getSocket();
  }

  /**{@inheritDoc}*/
  @Override
  public SSLSession getSSLSession() {
    return httpClientConnection.getSSLSession();
  }

  /**Gets the local address to which the socket is bound.*/
  @Override
  public InetAddress getLocalAddress() {
    return httpClientConnection.getLocalAddress();
  }

  /**Gets the local port to which the socket is bound.*/
  @Override
  public int getLocalPort() {
    return httpClientConnection.getLocalPort();
  }

  /**Returns the address to which the socket is connected.*/
  @Override
  public InetAddress getRemoteAddress() {
    return httpClientConnection.getRemoteAddress();
  }

  /**Returns the remote port number to which this socket is connected.*/
  @Override
  public int getRemotePort() {
    return httpClientConnection.getRemotePort();
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof AbfsManagedApacheHttpConnection) {
      return httpClientConnection.getId().equals(
          ((AbfsManagedApacheHttpConnection) o).httpClientConnection.getId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = targetHost != null ? new StringBuilder(
        targetHost.toString()) : new StringBuilder();
    stringBuilder
        .append(COLON)
        .append(hashCode());
    return stringBuilder.toString();
  }
}
