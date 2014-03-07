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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.webapp.Dispatcher;
import org.apache.hadoop.yarn.webapp.Router;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

@InterfaceAudience.LimitedPrivate({ "YARN", "MapReduce" })
@Singleton
public class RMDispatcher extends Dispatcher {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Inject
  RMDispatcher(WebApp webApp, Injector injector, Router router) {
    super(webApp, injector, router);
  }

  @Override
  public void service(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    res.setCharacterEncoding("UTF-8");
    String uri = HtmlQuoting.quoteHtmlChars(req.getRequestURI());

    if (uri == null) {
      uri = "/";
    }

    RMWebApp rmWebApp = (RMWebApp) webApp;
    rmWebApp.checkIfStandbyRM();
    if (rmWebApp.isStandby()
        && !uri.equals("/" + rmWebApp.name() + "/cluster")) {
      String redirectPath = rmWebApp.getRedirectPath() + uri;
      if (redirectPath != null && !redirectPath.isEmpty()) {
        String redirectMsg =
            "This is standby RM. Redirecting to the current active RM: "
                + redirectPath;
        res.addHeader("Refresh", "3; url=" + redirectPath);
        PrintWriter out = res.getWriter();
        out.println(redirectMsg);
        return;
      }
    }
    super.service(req, res);
  }
}
