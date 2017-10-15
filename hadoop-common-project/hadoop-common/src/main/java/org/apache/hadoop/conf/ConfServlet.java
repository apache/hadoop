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
package org.apache.hadoop.conf;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.Writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.HttpServer2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet to print out the running configuration data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class ConfServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  protected static final String FORMAT_JSON = "json";
  protected static final String FORMAT_XML = "xml";
  private static final String COMMAND = "cmd";
  private static final Logger LOG = LoggerFactory.getLogger(ConfServlet.class);
  private transient static final Configuration OZONE_CONFIG = new
      OzoneConfiguration();
  private transient Map<String, OzoneConfiguration.Property> propertyMap = null;


  /**
   * Return the Configuration of the daemon hosting this servlet.
   * This is populated when the HttpServer starts.
   */
  private Configuration getConfFromContext() {
    Configuration conf = (Configuration)getServletContext().getAttribute(
        HttpServer2.CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;
    return conf;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    // If user is a static user and auth Type is null, that means
    // there is a non-security environment and no need authorization,
    // otherwise, do the authorization.
    final ServletContext servletContext = getServletContext();
    if (!HttpServer2.isStaticUserAndNoneAuthType(servletContext, request) &&
        !HttpServer2.isInstrumentationAccessAllowed(servletContext,
            request, response)) {
      return;
    }

    String format = parseAcceptHeader(request);
    String cmd = request.getParameter(COMMAND);
    Writer out = response.getWriter();

    try {
      if (cmd == null) {
        if (FORMAT_XML.equals(format)) {
          response.setContentType("text/xml; charset=utf-8");
        } else if (FORMAT_JSON.equals(format)) {
          response.setContentType("application/json; charset=utf-8");
        }

        String name = request.getParameter("name");
        writeResponse(getConfFromContext(), out, format, name);
      } else {
        processConfigTagRequest(request, out);
      }
    } catch (BadFormatException bfe) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, bfe.getMessage());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, iae.getMessage());
    }
    out.close();
  }

  private void processConfigTagRequest(HttpServletRequest request,
      Writer out) throws IOException {
    String cmd = request.getParameter(COMMAND);
    Gson gson = new Gson();
    Configuration config = getOzoneConfig();

    config.get("ozone.enabled");
    switch (cmd) {
    case "getOzoneTags":
      LOG.debug(
          "Sending json for tags:" + gson.toJson(OzonePropertyTag.values()));
      out.write(gson.toJson(OzonePropertyTag.values()));
      break;
    case "getPropertyByTag":
      String tags = request.getParameter("tags");
      String tagGroup = request.getParameter("group");
      LOG.debug("Getting all properties for tags:" + tags + " group:" +
          tagGroup);
      List<PropertyTag> tagList = new ArrayList<>();
      for (String tag : tags.split(",")) {
        if (config.getPropertyTag(tag, tagGroup) != null) {
          tagList.add(config.getPropertyTag(tag, tagGroup));
        }
      }

      Properties properties = config.getAllPropertiesByTags(tagList);
      if (propertyMap == null) {
        loadDescriptions();
      }

      List<OzoneConfiguration.Property> filteredProperties = new ArrayList<>();

      properties.stringPropertyNames().stream().forEach(key -> {
        if (config.get(key) != null) {
          propertyMap.get(key).setValue(config.get(key));
          filteredProperties.add(propertyMap.get(key));
        }
      });
      out.write(gson.toJsonTree(filteredProperties).toString());
      break;
    default:
      throw new IllegalArgumentException(cmd + " is not a valid command.");
    }

  }

  private void loadDescriptions() {
    OzoneConfiguration config = (OzoneConfiguration) getOzoneConfig();
    List<OzoneConfiguration.Property> propList = null;
    propertyMap = new HashMap<>();
    try {
      propList = config.readPropertyFromXml(config.getResource("ozone-site"
          + ".xml"));
      propList.stream().map(p -> propertyMap.put(p.getName(), p));
      propList = config.readPropertyFromXml(config.getResource("ozone-default"
          + ".xml"));
      propList.stream().forEach(p -> {
        if (!propertyMap.containsKey(p.getName())) {
          propertyMap.put(p.getName(), p);
        }
      });
    } catch (Exception e) {
      LOG.error("Error while reading description from xml files", e);
    }
  }

  @VisibleForTesting
  static String parseAcceptHeader(HttpServletRequest request) {
    String format = request.getHeader(HttpHeaders.ACCEPT);
    return format != null && format.contains(FORMAT_JSON) ?
        FORMAT_JSON : FORMAT_XML;
  }

  /**
   * Guts of the servlet - extracted for easy testing.
   */
  static void writeResponse(Configuration conf,
      Writer out, String format, String propertyName)
          throws IOException, IllegalArgumentException, BadFormatException {
    if (FORMAT_JSON.equals(format)) {
      Configuration.dumpConfiguration(conf, propertyName, out);
    } else if (FORMAT_XML.equals(format)) {
      conf.writeXml(propertyName, out);
    } else {
      throw new BadFormatException("Bad format: " + format);
    }
  }

  static void writeResponse(Configuration conf, Writer out, String format)
      throws IOException, BadFormatException {
    writeResponse(conf, out, format, null);
  }

  public static class BadFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }

  private static Configuration getOzoneConfig() {
    return OZONE_CONFIG;
  }

}
