/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.client;

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

/**
 * A wrapper around HttpClient which provides some useful function and
 * semantics for interacting with the REST gateway.
 */
public class Client {
  public static final Header[] EMPTY_HEADER_ARRAY = new Header[0];

  private static final Log LOG = LogFactory.getLog(Client.class);

  private HttpClient httpClient;
  private Cluster cluster;

  /**
   * Default Constructor
   */
  public Client() {
    this(null);
  }

  /**
   * Constructor
   * @param cluster the cluster definition
   */
  public Client(Cluster cluster) {
    this.cluster = cluster;
    MultiThreadedHttpConnectionManager manager = 
      new MultiThreadedHttpConnectionManager();
    HttpConnectionManagerParams managerParams = manager.getParams();
    managerParams.setConnectionTimeout(2000); // 2 s
    managerParams.setDefaultMaxConnectionsPerHost(10);
    managerParams.setMaxTotalConnections(100);
    this.httpClient = new HttpClient(manager);
    HttpClientParams clientParams = httpClient.getParams();
    clientParams.setVersion(HttpVersion.HTTP_1_1);
  }

  /**
   * Shut down the client. Close any open persistent connections. 
   */
  public void shutdown() {
    MultiThreadedHttpConnectionManager manager = 
      (MultiThreadedHttpConnectionManager) httpClient.getHttpConnectionManager();
    manager.shutdown();
  }

  /**
   * Execute a transaction method given only the path. Will select at random
   * one of the members of the supplied cluster definition and iterate through
   * the list until a transaction can be successfully completed. The
   * definition of success here is a complete HTTP transaction, irrespective
   * of result code.  
   * @param cluster the cluster definition
   * @param method the transaction method
   * @param headers HTTP header values to send
   * @param path the path
   * @return the HTTP response code
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public int executePathOnly(Cluster cluster, HttpMethod method,
      Header[] headers, String path) throws IOException {
    IOException lastException;
    if (cluster.nodes.size() < 1) {
      throw new IOException("Cluster is empty");
    }
    int start = (int)Math.round((cluster.nodes.size() - 1) * Math.random());
    int i = start;
    do {
      cluster.lastHost = cluster.nodes.get(i);
      try {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(cluster.lastHost);
        sb.append(path);
        URI uri = new URI(sb.toString());
        return executeURI(method, headers, uri.toString());
      } catch (IOException e) {
        lastException = e;
      }
    } while (++i != start && i < cluster.nodes.size());
    throw lastException;
  }

  /**
   * Execute a transaction method given a complete URI.
   * @param method the transaction method
   * @param headers HTTP header values to send
   * @param uri the URI
   * @return the HTTP response code
   * @throws IOException
   */
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
      LOG.debug(method.getName() + " " + uri + " " + code + " " +
        method.getStatusText() + " in " + (endTime - startTime) + " ms");
    }
    return code;
  }

  /**
   * Execute a transaction method. Will call either <tt>executePathOnly</tt>
   * or <tt>executeURI</tt> depending on whether a path only is supplied in
   * 'path', or if a complete URI is passed instead, respectively.
   * @param cluster the cluster definition
   * @param method the HTTP method
   * @param headers HTTP header values to send
   * @param path the path or URI
   * @return the HTTP response code
   * @throws IOException
   */
  public int execute(Cluster cluster, HttpMethod method, Header[] headers,
      String path) throws IOException {
    if (path.startsWith("/")) {
      return executePathOnly(cluster, method, headers, path);
    }
    return executeURI(method, headers, path);
  }

  /**
   * @return the cluster definition
   */
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * @param cluster the cluster definition
   */
  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Send a HEAD request 
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response head(String path) throws IOException {
    return head(cluster, path, null);
  }

  /**
   * Send a HEAD request 
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response head(Cluster cluster, String path, Header[] headers) 
      throws IOException {
    HeadMethod method = new HeadMethod();
    try {
      int code = execute(cluster, method, null, path);
      headers = method.getResponseHeaders();
      return new Response(code, headers, null);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a GET request 
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path) throws IOException {
    return get(cluster, path);
  }

  /**
   * Send a GET request 
   * @param cluster the cluster definition
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster cluster, String path) throws IOException {
    return get(cluster, path, EMPTY_HEADER_ARRAY);
  }

  /**
   * Send a GET request 
   * @param path the path or URI
   * @param accept Accept header value
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path, String accept) throws IOException {
    return get(cluster, path, accept);
  }

  /**
   * Send a GET request 
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param accept Accept header value
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster cluster, String path, String accept)
      throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Accept", accept);
    return get(cluster, path, headers);
  }

  /**
   * Send a GET request
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request, 
   * <tt>Accept</tt> must be supplied
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path, Header[] headers) throws IOException {
    return get(cluster, path, headers);
  }

  /**
   * Send a GET request
   * @param c the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster c, String path, Header[] headers) 
      throws IOException {
    GetMethod method = new GetMethod();
    try {
      int code = execute(c, method, headers, path);
      headers = method.getResponseHeaders();
      byte[] body = method.getResponseBody();
      return new Response(code, headers, body);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a PUT request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(String path, String contentType, byte[] content)
      throws IOException {
    return put(cluster, path, contentType, content);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(Cluster cluster, String path, String contentType, 
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Content-Type", contentType);
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(String path, Header[] headers, byte[] content) 
      throws IOException {
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(Cluster cluster, String path, Header[] headers, 
      byte[] content) throws IOException {
    PutMethod method = new PutMethod();
    try {
      method.setRequestEntity(new ByteArrayRequestEntity(content));
      int code = execute(cluster, method, headers, path);
      headers = method.getResponseHeaders();
      content = method.getResponseBody();
      return new Response(code, headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a POST request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(String path, String contentType, byte[] content)
      throws IOException {
    return post(cluster, path, contentType, content);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(Cluster cluster, String path, String contentType, 
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new Header("Content-Type", contentType);
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(String path, Header[] headers, byte[] content) 
      throws IOException {
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(Cluster cluster, String path, Header[] headers, 
      byte[] content) throws IOException {
    PostMethod method = new PostMethod();
    try {
      method.setRequestEntity(new ByteArrayRequestEntity(content));
      int code = execute(cluster, method, headers, path);
      headers = method.getResponseHeaders();
      content = method.getResponseBody();
      return new Response(code, headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a DELETE request
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response delete(String path) throws IOException {
    return delete(cluster, path);
  }

  /**
   * Send a DELETE request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response delete(Cluster cluster, String path) throws IOException {
    DeleteMethod method = new DeleteMethod();
    try {
      int code = execute(cluster, method, null, path);
      Header[] headers = method.getResponseHeaders();
      byte[] content = method.getResponseBody();
      return new Response(code, headers, content);
    } finally {
      method.releaseConnection();
    }
  }

}
