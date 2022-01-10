/*
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
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Servlet for disabled async-profiler.
 */
@InterfaceAudience.Private
public class ProfilerDisabledServlet extends HttpServlet {

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
    resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    ProfileServlet.setResponseHeader(resp);
    // TODO : Replace github.com link with
    //  https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/
    //  AsyncProfilerServlet.html once Async profiler changes are released
    //  in 3.x (3.4.0 as of today).
    resp.getWriter().write("The profiler servlet was disabled at startup.\n\n"
        + "Please ensure the prerequisites for the Profiler Servlet have been installed and the\n"
        + "environment is properly configured. \n\n"
        + "For more details, please refer to: https://github.com/apache/hadoop/blob/trunk/"
        + "hadoop-common-project/hadoop-common/src/site/markdown/AsyncProfilerServlet.md");
  }

}
