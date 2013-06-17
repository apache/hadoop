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

package org.apache.hadoop.yarn.webapp.view;

import com.google.common.base.Joiner;
import java.util.Enumeration;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class DefaultPage extends TextPage {
  static final Joiner valJoiner = Joiner.on(", ");

  @Override
  public void render() {
    puts("Request URI: ", request().getRequestURI());
    puts("Query parameters:");
    @SuppressWarnings("unchecked")
    Map<String, String[]> params = request().getParameterMap();
    for (Map.Entry<String, String[]> e : params.entrySet()) {
      puts("  ", e.getKey(), "=", valJoiner.join(e.getValue()));
    }
    puts("More parameters:");
    for (Map.Entry<String, String> e : moreParams().entrySet()) {
      puts("  ", e.getKey(), "=", e.getValue());
    }
    puts("Path info: ", request().getPathInfo());
    puts("Path translated: ", request().getPathTranslated());
    puts("Auth type: ", request().getAuthType());
    puts("Remote address: "+ request().getRemoteAddr());
    puts("Remote user: ", request().getRemoteUser());
    puts("Servlet attributes:");
    @SuppressWarnings("unchecked")
    Enumeration<String> attrNames = request().getAttributeNames();
    while (attrNames.hasMoreElements()) {
      String key = attrNames.nextElement();
      puts("  ", key, "=", request().getAttribute(key));
    }
    puts("Headers:");
    @SuppressWarnings("unchecked")
    Enumeration<String> headerNames = request().getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String key = headerNames.nextElement();
      puts("  ", key, "=", request().getHeader(key));
    }
  }
}
