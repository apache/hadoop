/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.monitoring;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorStatus;
import org.apache.hadoop.hbase.util.VersionInfo;

public abstract class StateDumpServlet extends HttpServlet {
  static final long DEFAULT_TAIL_KB = 100;
  private static final long serialVersionUID = 1L;

  protected void dumpVersionInfo(PrintWriter out) {
    VersionInfo.writeTo(out);
  
    out.println("Hadoop " + org.apache.hadoop.util.VersionInfo.getVersion());
    out.println("Subversion " + org.apache.hadoop.util.VersionInfo.getUrl() + " -r " +
        org.apache.hadoop.util.VersionInfo.getRevision());
    out.println("Compiled by " + org.apache.hadoop.util.VersionInfo.getUser() +
        " on " + org.apache.hadoop.util.VersionInfo.getDate());
  }

  protected long getTailKbParam(HttpServletRequest request) {
    String param = request.getParameter("tailkb");
    if (param == null) {
      return DEFAULT_TAIL_KB;
    }
    return Long.parseLong(param);
  }

  protected void dumpExecutors(ExecutorService service, PrintWriter out)
      throws IOException {
    Map<String, ExecutorStatus> statuses = service.getAllExecutorStatuses();
    for (ExecutorStatus status : statuses.values()) {
      status.dumpTo(out, "  ");
    }
  }
}