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
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

public class GZIPResponseStream extends ServletOutputStream
{
  private HttpServletResponse response;
  private GZIPOutputStream out;

  public GZIPResponseStream(HttpServletResponse response) throws IOException {
    this.response = response;
    this.out = new GZIPOutputStream(response.getOutputStream());
    response.addHeader("Content-Encoding", "gzip");
  }

  public void resetBuffer() {
    if (out != null && !response.isCommitted()) {
      response.setHeader("Content-Encoding", null);
    }
    out = null;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    finish();
    out.close();
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  public void finish() throws IOException {
    out.finish();
  }
}