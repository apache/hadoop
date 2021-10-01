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

package org.apache.hadoop.yarn.webapp;

import com.google.inject.Injector;
import com.sun.istack.NotNull;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

@Singleton
public class RemoveGeneratedByJerseyFromApplicationWADLFilter extends GuiceContainer {

  @Inject
  public RemoveGeneratedByJerseyFromApplicationWADLFilter(Injector injector) {
    super(injector);
  }

  @Override
  public void doFilter(HttpServletRequest request,
                       HttpServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    ResponseBodyCopier copier = new ResponseBodyCopier(response);
    chain.doFilter(request, copier);

    String originalContent = copier.getResponseBody();
    byte[] alteredContent = originalContent.replaceFirst("jersey:generatedBy=\".*\"", "")
            .getBytes(StandardCharsets.UTF_8);

    response.setContentLength(alteredContent.length);
    response.resetBuffer();
    response.getOutputStream().write(alteredContent);
    response.getOutputStream().flush();
  }

  public static class ResponseBodyCopier extends HttpServletResponseWrapper {
    private final ByteArrayOutputStream byteOutputStream;
    private final ServletOutputStream out;
    private final PrintWriter writer;

    public ResponseBodyCopier(HttpServletResponse resp) {
      super(resp);
      this.byteOutputStream = new ByteArrayOutputStream();
      this.out = new SubServletOutputStream(byteOutputStream);
      this.writer = new PrintWriter(new OutputStreamWriter(byteOutputStream,
          StandardCharsets.UTF_8));
    }

    @Override
    public ServletOutputStream getOutputStream() {
      return out;
    }

    @Override
    public PrintWriter getWriter() {
      return writer;
    }

    public String getResponseBody() throws IOException {
      out.flush();
      writer.flush();
      return new String(byteOutputStream.toByteArray(), StandardCharsets.UTF_8);
    }

    static class SubServletOutputStream extends ServletOutputStream {
      private final ByteArrayOutputStream byteOutputStream;

      SubServletOutputStream(ByteArrayOutputStream byteOutputStream) {
        this.byteOutputStream = byteOutputStream;
      }

      @Override
      public void write(int b) {
        byteOutputStream.write(b);
      }

      @Override
      public void write(@NotNull byte[] b) {
        byteOutputStream.write(b, 0, b.length);
      }

      @Override
      public boolean isReady() {
        return false;
      }

      @Override
      public void setWriteListener(WriteListener writeListener) {
      }
    }
  }
}
