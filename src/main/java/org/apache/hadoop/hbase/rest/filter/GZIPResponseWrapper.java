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

package org.apache.hadoop.hbase.rest.filter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

public class GZIPResponseWrapper extends HttpServletResponseWrapper {
  private HttpServletResponse response;
  private ServletOutputStream os;
  private PrintWriter writer;
  private boolean compress = true;

  public GZIPResponseWrapper(HttpServletResponse response) {
    super(response);
    this.response = response;
  }

  @Override
  public void setStatus(int status) {
    super.setStatus(status);
    if (status < 200 || status >= 300) {
      compress = false;
    }
  }

  @Override
  public void addHeader(String name, String value) {
    if (!"content-length".equalsIgnoreCase(name)) {
      super.addHeader(name, value);
    }
  }

  @Override
  public void setContentLength(int length) {
    // do nothing
  }

  @Override
  public void setIntHeader(String name, int value) {
    if (!"content-length".equalsIgnoreCase(name)) {
      super.setIntHeader(name, value);
    }
  }

  @Override
  public void setHeader(String name, String value) {
    if (!"content-length".equalsIgnoreCase(name)) {
      super.setHeader(name, value);
    }
  }

  @Override
  public void flushBuffer() throws IOException {
    if (writer != null) {
      writer.flush();
    }
    if (os != null && (os instanceof GZIPResponseStream)) {
      ((GZIPResponseStream)os).finish();
    } else {
      getResponse().flushBuffer();
    }
  }

  @Override
  public void reset() {
    super.reset();
    if (os != null && (os instanceof GZIPResponseStream)) {
      ((GZIPResponseStream)os).resetBuffer();
    }
    writer = null;
    os = null;
    compress = true;
  }

  @Override
  public void resetBuffer() {
    super.resetBuffer();
    if (os != null && (os instanceof GZIPResponseStream)) {
      ((GZIPResponseStream)os).resetBuffer();
    }
    writer = null;
    os = null;
  }

  @Override
  public void sendError(int status, String msg) throws IOException {
    resetBuffer();
    super.sendError(status, msg);
  }

  @Override
  public void sendError(int status) throws IOException {
    resetBuffer();
    super.sendError(status);
  }

  @Override
  public void sendRedirect(String location) throws IOException {
    resetBuffer();
    super.sendRedirect(location);
  }

  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    if (os == null) {
      if (!response.isCommitted() && compress) {
        os = (ServletOutputStream)new GZIPResponseStream(response);
      } else {
        os = response.getOutputStream();
      }
    }
    return os;
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    if (writer == null) {
      writer = new PrintWriter(getOutputStream());
    }
    return writer;
  }
}