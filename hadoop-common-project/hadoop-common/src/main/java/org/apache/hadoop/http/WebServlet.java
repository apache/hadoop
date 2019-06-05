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
package org.apache.hadoop.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hadoop DefaultServlet for serving static web content.
 */
public class WebServlet extends DefaultServlet {
  private static final long serialVersionUID = 3910031415927L;
  public static final Logger LOG = LoggerFactory.getLogger(WebServlet.class);

  /**
   * Get method is modified to support impersonation and Kerberos
   * SPNEGO token by forcing client side redirect when accessing
   * "/" (root) of the web application context.
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (request.getRequestURI().equals("/")) {
      StringBuilder location = new StringBuilder();
      location.append("index.html");
      if (request.getQueryString()!=null) {
        // echo query string but prevent HTTP response splitting
        location.append("?");
        location.append(request.getQueryString()
            .replaceAll("\n", "").replaceAll("\r", ""));
      }
      response.sendRedirect(location.toString());
    } else {
      super.doGet(request, response);
    }
  }
}
