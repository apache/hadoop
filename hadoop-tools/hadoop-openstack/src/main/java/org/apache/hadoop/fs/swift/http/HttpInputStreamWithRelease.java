/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.http;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionClosedException;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * This replaces the input stream release class from JetS3t and AWS;
 * # Failures in the constructor are relayed up instead of simply logged.
 * # it is set up to be more robust at teardown
 * # release logic is thread safe
 * Note that the thread safety of the inner stream contains no thread
 * safety guarantees -this stream is not to be read across streams.
 * The thread safety logic here is to ensure that even if somebody ignores
 * that rule, the release code does not get entered twice -and that
 * any release in one thread is picked up by read operations in all others.
 */
public class HttpInputStreamWithRelease extends InputStream {

  private static final Log LOG =
    LogFactory.getLog(HttpInputStreamWithRelease.class);
  private final URI uri;
  private HttpMethod method;
  //flag to say the stream is released -volatile so that read operations
  //pick it up even while unsynchronized.
  private volatile boolean released;
  //volatile flag to verify that data is consumed.
  private volatile boolean dataConsumed;
  private InputStream inStream;
  /**
   * In debug builds, this is filled in with the construction-time
   * stack, which is then included in logs from the finalize(), method.
   */
  private final Exception constructionStack;

  /**
   * Why the stream is closed
   */
  private String reasonClosed = "unopened";

  public HttpInputStreamWithRelease(URI uri, HttpMethod method) throws
                                                                IOException {
    this.uri = uri;
    this.method = method;
    constructionStack = LOG.isDebugEnabled() ? new Exception("stack") : null;
    if (method == null) {
      throw new IllegalArgumentException("Null 'method' parameter ");
    }
    try {
      inStream = method.getResponseBodyAsStream();
    } catch (IOException e) {
      inStream = new ByteArrayInputStream(new byte[]{});
      throw releaseAndRethrow("getResponseBodyAsStream() in constructor -" + e, e);
    }
  }

  @Override
  public void close() throws IOException {
    release("close()", null);
  }

  /**
   * Release logic
   * @param reason reason for release (used in debug messages)
   * @param ex exception that is a cause -null for non-exceptional releases
   * @return true if the release took place here
   * @throws IOException if the abort or close operations failed.
   */
  private synchronized boolean release(String reason, Exception ex) throws
                                                                   IOException {
    if (!released) {
      reasonClosed = reason;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Releasing connection to " + uri + ":  " + reason, ex);
        }
        if (method != null) {
          if (!dataConsumed) {
            method.abort();
          }
          method.releaseConnection();
        }
        if (inStream != null) {
          //this guard may seem un-needed, but a stack trace seen
          //on the JetS3t predecessor implied that it
          //is useful
          inStream.close();
        }
        return true;
      } finally {
        //if something went wrong here, we do not want the release() operation
        //to try and do anything in advance.
        released = true;
        dataConsumed = true;
      }
    } else {
      return false;
    }
  }

  /**
   * Release the method, using the exception as a cause
   * @param operation operation that failed
   * @param ex the exception which triggered it.
   * @return the exception to throw
   */
  private IOException releaseAndRethrow(String operation, IOException ex) {
    try {
      release(operation, ex);
    } catch (IOException ioe) {
      LOG.debug("Exception during release: " + operation + " - " + ioe, ioe);
      //make this the exception if there was none before
      if (ex == null) {
        ex = ioe;
      }
    }
    return ex;
  }

  /**
   * Assume that the connection is not released: throws an exception if it is
   * @throws SwiftConnectionClosedException
   */
  private synchronized void assumeNotReleased() throws SwiftConnectionClosedException {
    if (released || inStream == null) {
      throw new SwiftConnectionClosedException(reasonClosed);
    }
  }

  @Override
  public int available() throws IOException {
    assumeNotReleased();
    try {
      return inStream.available();
    } catch (IOException e) {
      throw releaseAndRethrow("available() failed -" + e, e);
    }
  }

  @Override
  public int read() throws IOException {
    assumeNotReleased();
    int read = 0;
    try {
      read = inStream.read();
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("EOF exception " + e, e);
      }
      read = -1;
    } catch (IOException e) {
      throw releaseAndRethrow("read()", e);
    }
    if (read < 0) {
      dataConsumed = true;
      release("read() -all data consumed", null);
    }
    return read;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    SwiftUtils.validateReadArgs(b, off, len);
    if (len == 0) {
      return 0;
    }
    //if the stream is already closed, then report an exception.
    assumeNotReleased();
    //now read in a buffer, reacting differently to different operations
    int read;
    try {
      read = inStream.read(b, off, len);
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("EOF exception " + e, e);
      }
      read = -1;
    } catch (IOException e) {
      throw releaseAndRethrow("read(b, off, " + len + ")", e);
    }
    if (read < 0) {
      dataConsumed = true;
      release("read() -all data consumed", null);
    }
    return read;
  }

  /**
   * Finalizer does release the stream, but also logs at WARN level
   * including the URI at fault
   */
  @Override
  protected void finalize() {
    try {
      if (release("finalize()", constructionStack)) {
        LOG.warn("input stream of " + uri
                 + " not closed properly -cleaned up in finalize()");
      }
    } catch (Exception e) {
      //swallow anything that failed here
      LOG.warn("Exception while releasing " + uri + "in finalizer",
               e);
    }
  }

  @Override
  public String toString() {
    return "HttpInputStreamWithRelease working with " + uri
      +" released=" + released
      +" dataConsumed=" + dataConsumed;
  }
}
