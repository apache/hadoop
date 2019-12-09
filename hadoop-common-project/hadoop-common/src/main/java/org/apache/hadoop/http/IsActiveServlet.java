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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Used by Load Balancers to detect the active NameNode/ResourceManager/Router.
 */
public abstract class IsActiveServlet extends HttpServlet {

  /** Default serial identifier. */
  private static final long serialVersionUID = 1L;

  public static final String SERVLET_NAME = "isActive";
  public static final String PATH_SPEC = "/isActive";

  public static final String RESPONSE_ACTIVE =
      "I am Active!";

  public static final String RESPONSE_NOT_ACTIVE =
      "I am not Active!";

  /**
   * Check whether this instance is the Active one.
   * @param req HTTP request
   * @param resp HTTP response to write to
   */
  @Override
  public void doGet(
      final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {

    // By default requests are persistent. We don't want long-lived connections
    // on server side.
    resp.addHeader("Connection", "close");

    if (!isActive()) {
      // Report not SC_OK
      resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
          RESPONSE_NOT_ACTIVE);
      return;
    }
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().write(RESPONSE_ACTIVE);
    resp.getWriter().flush();
  }

  /**
   * @return true if this instance is in Active HA state.
   */
  protected abstract boolean isActive();
}
