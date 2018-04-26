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
package org.apache.hadoop.ozone.web.netty;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.TRANSFER_ENCODING;
import static io.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseWriter;
import com.sun.jersey.spi.container.WebApplication;

import io.netty.handler.codec.http.DefaultHttpResponse;
//import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.handlers.StorageHandlerBuilder;

/**
 * This is a custom Jersey container that hosts the Object Store web
 * application. It supports dispatching an inbound Netty {@link HttpRequest}
 * to the Object Store Jersey application.  Request dispatching must run
 * asynchronously, because the Jersey application must consume the inbound
 * HTTP request from a  piped stream and produce the outbound HTTP response
 * for another piped stream.The Netty channel handlers consume the connected
 * ends of these piped streams. Request dispatching cannot run directly on
 * the Netty threads, or there would be a risk of deadlock (one thread
 * producing/consuming its end of the pipe  while no other thread is
 * producing/consuming the opposite end).
 */
public final class ObjectStoreJerseyContainer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreJerseyContainer.class);

  private final WebApplication webapp;

  private StorageHandler storageHandler;

  /**
   * Creates a new ObjectStoreJerseyContainer.
   *
   * @param webapp web application
   */
  public ObjectStoreJerseyContainer(WebApplication webapp) {
    this.webapp = webapp;
  }

  /**
   * Sets the {@link StorageHandler}. This must be called before dispatching any
   * requests.
   *
   * @param newStorageHandler {@link StorageHandler} implementation
   */
  public void setStorageHandler(StorageHandler newStorageHandler) {
    this.storageHandler = newStorageHandler;
  }

  /**
   * Asynchronously executes an HTTP request.
   *
   * @param nettyReq HTTP request
   * @param reqIn input stream for reading request body
   * @param respOut output stream for writing response body
   */
  public Future<HttpResponse> dispatch(HttpRequest nettyReq, InputStream reqIn,
                                       OutputStream respOut) {
    // The request executes on a separate background thread.  As soon as enough
    // processing has completed to bootstrap the outbound response, the thread
    // counts down on a latch.  This latch also unblocks callers trying to get
    // the asynchronous response out of the returned future.
    final CountDownLatch latch = new CountDownLatch(1);
    final RequestRunner runner = new RequestRunner(nettyReq, reqIn, respOut,
        latch);
    final Thread thread = new Thread(runner);
    thread.setDaemon(true);
    thread.start();
    return new Future<HttpResponse>() {

      private volatile boolean isCancelled = false;

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (latch.getCount() == 0) {
          return false;
        }
        if (!mayInterruptIfRunning) {
          return false;
        }
        if (!thread.isAlive()) {
          return false;
        }
        thread.interrupt();
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.info("Interrupted while attempting to cancel dispatch thread.");
          Thread.currentThread().interrupt();
          return false;
        }
        isCancelled = true;
        return true;
      }

      @Override
      public HttpResponse get()
          throws InterruptedException, ExecutionException {
        checkCancelled();
        latch.await();
        return this.getOrThrow();
      }

      @Override
      public HttpResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        checkCancelled();
        if (!latch.await(timeout, unit)) {
          throw new TimeoutException(String.format(
              "Timed out waiting for HttpResponse after %d %s.",
              timeout, unit.toString().toLowerCase()));
        }
        return this.getOrThrow();
      }

      @Override
      public boolean isCancelled() {
        return isCancelled;
      }

      @Override
      public boolean isDone() {
        return !isCancelled && latch.getCount() == 0;
      }

      private void checkCancelled() {
        if (isCancelled()) {
          throw new CancellationException();
        }
      }

      private HttpResponse getOrThrow() throws ExecutionException {
        try {
          return runner.getResponse();
        } catch (Exception e) {
          throw new ExecutionException(e);
        }
      }
    };
  }

  /**
   * Runs the actual handling of the HTTP request.
   */
  private final class RequestRunner implements Runnable,
      ContainerResponseWriter {

    private final CountDownLatch latch;
    private final HttpRequest nettyReq;
    private final InputStream reqIn;
    private final OutputStream respOut;

    private Exception exception;
    private HttpResponse nettyResp;

    /**
     * Creates a new RequestRunner.
     *
     * @param nettyReq HTTP request
     * @param reqIn input stream for reading request body
     * @param respOut output stream for writing response body
     * @param latch for coordinating asynchronous return of HTTP response
     */
    RequestRunner(HttpRequest nettyReq, InputStream reqIn,
                         OutputStream respOut, CountDownLatch latch) {
      this.latch = latch;
      this.nettyReq = nettyReq;
      this.reqIn = reqIn;
      this.respOut = respOut;
    }

    @Override
    public void run() {
      LOG.trace("begin RequestRunner, nettyReq = {}", this.nettyReq);
      StorageHandlerBuilder.setStorageHandler(
          ObjectStoreJerseyContainer.this.storageHandler);
      try {
        ContainerRequest jerseyReq = nettyRequestToJerseyRequest(
            ObjectStoreJerseyContainer.this.webapp, this.nettyReq, this.reqIn);
        ObjectStoreJerseyContainer.this.webapp.handleRequest(jerseyReq, this);
      } catch (Exception e) {
        LOG.error("Error running Jersey Request Runner", e);
        this.exception = e;
        this.latch.countDown();
      } finally {
        IOUtils.cleanupWithLogger(null, this.reqIn, this.respOut);
        StorageHandlerBuilder.removeStorageHandler();
      }
      LOG.trace("end RequestRunner, nettyReq = {}", this.nettyReq);
    }

    /**
     * This is a callback triggered by Jersey as soon as dispatch has completed
     * to the point of knowing what kind of response to return.  We save the
     * response and trigger the latch to unblock callers waiting on the
     * asynchronous return of the response.  Our response always sets a
     * Content-Length header.  (We do not support Transfer-Encoding: chunked.)
     * We also return the output stream for Jersey to use for writing the
     * response body.
     *
     * @param contentLength length of response
     * @param jerseyResp HTTP response returned by Jersey
     * @return OutputStream for Jersey to use for writing the response body
     */
    @Override
    public OutputStream writeStatusAndHeaders(long contentLength,
                                              ContainerResponse jerseyResp) {
      LOG.trace(
          "begin writeStatusAndHeaders, contentLength = {}, jerseyResp = {}.",
          contentLength, jerseyResp);
      this.nettyResp = jerseyResponseToNettyResponse(jerseyResp);
      this.nettyResp.headers().set(CONTENT_LENGTH, Math.max(0, contentLength));
      this.nettyResp.headers().set(CONNECTION,
          HttpHeaders.isKeepAlive(this.nettyReq) ? KEEP_ALIVE : CLOSE);
      this.latch.countDown();
      LOG.trace(
          "end writeStatusAndHeaders, contentLength = {}, jerseyResp = {}.",
          contentLength, jerseyResp);
      return this.respOut;
    }

    /**
     * This is a callback triggered by Jersey after it has completed writing the
     * response body to the stream.  We must close the stream here to unblock
     * the Netty thread consuming the last chunk of the response from the input
     * end of the piped stream.
     *
     * @throws IOException if there is an I/O error
     */
    @Override
    public void finish() throws IOException {
      IOUtils.cleanupWithLogger(null, this.respOut);
    }

    /**
     * Gets the HTTP response calculated by the Jersey application, or throws an
     * exception if an error occurred during processing.  It only makes sense to
     * call this method after waiting on the latch to trigger.
     *
     * @return HTTP response
     * @throws Exception if there was an error executing the request
     */
    public HttpResponse getResponse() throws Exception {
      if (this.exception != null) {
        throw this.exception;
      }
      return this.nettyResp;
    }
  }

  /**
   * Converts a Jersey HTTP response object to a Netty HTTP response object.
   *
   * @param jerseyResp Jersey HTTP response
   * @return Netty HTTP response
   */
  private static HttpResponse jerseyResponseToNettyResponse(
      ContainerResponse jerseyResp) {
    HttpResponse nettyResp = new DefaultHttpResponse(HTTP_1_1,
        HttpResponseStatus.valueOf(jerseyResp.getStatus()));
    for (Map.Entry<String, List<Object>> header :
        jerseyResp.getHttpHeaders().entrySet()) {
      if (!header.getKey().equalsIgnoreCase(CONTENT_LENGTH.toString()) &&
          !header.getKey().equalsIgnoreCase(TRANSFER_ENCODING.toString())) {
        nettyResp.headers().set(header.getKey(), header.getValue());
      }
    }
    return nettyResp;
  }

  /**
   * Converts a Netty HTTP request object to a Jersey HTTP request object.
   *
   * @param webapp web application
   * @param nettyReq Netty HTTP request
   * @param reqIn input stream for reading request body
   * @return Jersey HTTP request
   * @throws URISyntaxException if there is an error handling the request URI
   */
  private static ContainerRequest nettyRequestToJerseyRequest(
      WebApplication webapp, HttpRequest nettyReq, InputStream reqIn)
      throws URISyntaxException {
    HttpHeaders nettyHeaders = nettyReq.headers();
    InBoundHeaders jerseyHeaders = new InBoundHeaders();
    for (String name : nettyHeaders.names()) {
      jerseyHeaders.put(name, nettyHeaders.getAll(name));
    }
    String host = nettyHeaders.get(HOST);
    String scheme = host.startsWith("https") ? "https://" : "http://";
    String baseUri = scheme + host + "/";
    String reqUri = scheme + host + nettyReq.getUri();
    LOG.trace("baseUri = {}, reqUri = {}", baseUri, reqUri);
    return new ContainerRequest(webapp, nettyReq.getMethod().name(),
        new URI(baseUri), new URI(reqUri), jerseyHeaders, reqIn);
  }
}
