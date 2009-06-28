/*
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.client;

import java.io.IOException;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Client {
  public static final Header[] EMPTY_HEADER_ARRAY = new Header[0];

  private static final Log LOG = LogFactory.getLog(Client.class);

  private HttpClient httpClient;
  private Cluster cluster;

  public Client() {
    this(null);
  }

  public Client(Cluster cluster) {
    this.cluster = cluster;
    httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
    HttpConnectionManagerParams managerParams =
      httpClient.getHttpConnectionManager().getParams();
    managerParams.setConnectionTimeout(2000); // 2 s
    HttpClientParams clientParams = httpClient.getParams();
    clientParams.setVersion(HttpVersion.HTTP_1_1);
  }

  public void shutdown() {
    MultiThreadedHttpConnectionManager manager = 
      (MultiThreadedHttpConnectionManager) httpClient.getHttpConnectionManager();
    manager.shutdown();
  }

  @SuppressWarnings("deprecation")
  public int executePathOnly(Cluster c, HttpMethod method, Header[] headers,
      String path) throws IOException {
    IOException lastException;
    if (c.nodes.size() < 1) {
      throw new IOException("Cluster is empty");
    }
    int start = (int)Math.round((c.nodes.size() - 1) * Math.random());
    int i = start;
    do {
      c.lastHost = c.nodes.get(i);
      try {
        StringBuffer sb = new StringBuffer();
        sb.append("http://");
        sb.append(c.lastHost);
        sb.append(path);
        URI uri = new URI(sb.toString());
        return executeURI(method, headers, uri.toString());
      } catch (IOException e) {
        lastException = e;
      }
    } while (++i != start && i < c.nodes.size());
    throw lastException;
  }

  @SuppressWarnings("deprecation")
  public int executeURI(HttpMethod method, Header[] headers, String uri)
      throws IOException {
    method.setURI(new URI(uri));
    if (headers != null) {
      for (Header header: headers) {
        method.addRequestHeader(header);
      }
    }
    long startTime = System.currentTimeMillis();
    int code = httpClient.executeMethod(method);
    long endTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug(method.getName() + " " + uri + ": " + code + " " +
        method.getStatusText() + " in " + (endTime - startTime) + " ms");
    }
    return code;
  }

  public int execute(Cluster c, HttpMethod method, Header[] headers,
      String path) throws IOException {
    if (path.startsWith("/")) {
      return executePathOnly(c, method, headers, path);
    }
    return executeURI(method, headers, path);
  }

  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public Response head(String path) throws IOException {
    return head(cluster, path);
  }

  public Response head(Cluster c, String path) throws IOException {
    HeadMethod method = new HeadMethod();
    int code = execute(c, method, null, path);
    Header[] headers = method.getResponseHeaders();
    method.releaseConnection();
    return new Response(code, headers, null);
  }

  public Response get(String path) throws IOException {
    return get(cluster, path);
  }

  public Response get(Cluster c, String path) throws IOException {
    return get(c, path, EMPTY_HEADER_ARRAY);
  }

  public Response get(String path, String accept) throws IOException {
    return get(cluster, path, accept);
  }

  public Response get(Cluster c, String path, String accept)
      throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Accept", accept);
    return get(c, path, headers);
  }

  public Response get(String path, Header[] headers) throws IOException {
    return get(cluster, path, headers);
  }

  public Response get(Cluster c, String path, Header[] headers) 
      throws IOException {
    GetMethod method = new GetMethod();
    int code = execute(c, method, headers, path);
    headers = method.getResponseHeaders();
    byte[] body = method.getResponseBody();
    method.releaseConnection();
    return new Response(code, headers, body);
  }

  public Response put(String path, String contentType, byte[] content)
      throws IOException {
    return put(cluster, path, contentType, content);
  }

  public Response put(Cluster c, String path, String contentType, 
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Content-Type", contentType);
    return put(c, path, headers, content);
  }

  public Response put(String path, Header[] headers, byte[] body) 
      throws IOException {
    return put(cluster, path, headers, body);
  }

  public Response put(Cluster c, String path, Header[] headers, 
      byte[] body) throws IOException {
    PutMethod method = new PutMethod();
    method.setRequestEntity(new ByteArrayRequestEntity(body));
    int code = execute(c, method, headers, path);
    headers = method.getResponseHeaders();
    body = method.getResponseBody();
    method.releaseConnection();
    return new Response(code, headers, body);
  }

  public Response post(String path, String contentType, byte[] content)
      throws IOException {
    return post(cluster, path, contentType, content);
  }

  public Response post(Cluster c, String path, String contentType, 
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Content-Type", contentType);
    return post(c, path, headers, content);
  }

  public Response post(String path, Header[] headers, byte[] content) 
      throws IOException {
    return post(cluster, path, headers, content);
  }

  public Response post(Cluster c, String path, Header[] headers, 
      byte[] content) throws IOException {
    PostMethod method = new PostMethod();
    method.setRequestEntity(new ByteArrayRequestEntity(content));
    int code = execute(c, method, headers, path);
    headers = method.getResponseHeaders();
    content = method.getResponseBody();
    method.releaseConnection();
    return new Response(code, headers, content);
  }

  public Response delete(String path) throws IOException {
    return delete(cluster, path);
  }

  public Response delete(Cluster c, String path) throws IOException {
    DeleteMethod method = new DeleteMethod();
    int code = execute(c, method, null, path);
    Header[] headers = method.getResponseHeaders();
    method.releaseConnection();
    return new Response(code, headers);
  }
}
