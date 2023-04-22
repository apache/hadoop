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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestLog object for use with Http
 */
public class HttpRequestLog {

  public static final Logger LOG =
      LoggerFactory.getLogger(HttpRequestLog.class);
  private static final Map<String, String> serverToComponent;

  static {
    Map<String, String > map = new HashMap<String, String>();
    map.put("cluster", "resourcemanager");
    map.put("hdfs", "namenode");
    map.put("node", "nodemanager");
    serverToComponent = Collections.unmodifiableMap(map);
  }

  public static RequestLog getRequestLog(String name) {
    String lookup = serverToComponent.get(name);
    if (lookup != null) {
      name = lookup;
    }
    String loggerName = "http.requests." + name;
    Slf4jRequestLogWriter writer = new Slf4jRequestLogWriter();
    writer.setLoggerName(loggerName);
    return new CustomRequestLog(writer, CustomRequestLog.EXTENDED_NCSA_FORMAT);
  }

  private HttpRequestLog() {
  }
}
