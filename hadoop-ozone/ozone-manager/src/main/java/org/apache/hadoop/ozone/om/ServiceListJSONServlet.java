/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * Provides REST access to Ozone Service List.
 * <p>
 * This servlet generally will be placed under the /serviceList URL of
 * OzoneManager HttpServer.
 *
 * The return format is of JSON and in the form
 * <p>
 *  <pre><code>
 *  {
 *    "services" : [
 *      {
 *        "NodeType":"OM",
 *        "Hostname" "$hostname",
 *        "ports" : {
 *          "$PortType" : "$port",
 *          ...
 *        }
 *      }
 *    ]
 *  }
 *  </code></pre>
 *  <p>
 *
 */
public class ServiceListJSONServlet  extends HttpServlet  {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceListJSONServlet.class);
  private static final long serialVersionUID = 1L;

  private transient OzoneManager om;

  @Override
  public void init() throws ServletException {
    this.om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
  }

  /**
   * Process a GET request for the specified resource.
   *
   * @param request
   *          The servlet request we are processing
   * @param response
   *          The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      response.setContentType("application/json; charset=utf8");
      PrintWriter writer = response.getWriter();
      try {
        writer.write(objectMapper.writeValueAsString(om.getServiceList()));
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    } catch (IOException e) {
      LOG.error(
          "Caught an exception while processing ServiceList request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

}
