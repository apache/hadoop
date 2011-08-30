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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.monitoring.LogMonitoring;
import org.apache.hadoop.hbase.monitoring.StateDumpServlet;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.util.ReflectionUtils;

public class MasterDumpServlet extends StateDumpServlet {
  private static final long serialVersionUID = 1L;
  private static final String LINE =
    "===========================================================";
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    assert master != null : "No Master in context!";

    response.setContentType("text/plain");
    OutputStream os = response.getOutputStream();
    PrintWriter out = new PrintWriter(os);
    
    out.println("Master status for " + master.getServerName()
        + " as of " + new Date());
    
    out.println("\n\nVersion Info:");
    out.println(LINE);
    dumpVersionInfo(out);

    out.println("\n\nTasks:");
    out.println(LINE);
    TaskMonitor.get().dumpAsText(out);
    
    out.println("\n\nServers:");
    out.println(LINE);
    dumpServers(master, out);
    
    out.println("\n\nRegions-in-transition:");
    out.println(LINE);
    dumpRIT(master, out);
    
    out.println("\n\nExecutors:");
    out.println(LINE);
    dumpExecutors(master.getExecutorService(), out);
    
    out.println("\n\nStacks:");
    out.println(LINE);
    ReflectionUtils.printThreadInfo(out, "");
    
    out.println("\n\nMaster configuration:");
    out.println(LINE);
    Configuration conf = master.getConfiguration();
    out.flush();
    conf.writeXml(os);
    os.flush();
    
    out.println("\n\nRecent regionserver aborts:");
    out.println(LINE);
    master.getRegionServerFatalLogBuffer().dumpTo(out);
    
    out.println("\n\nLogs");
    out.println(LINE);
    long tailKb = getTailKbParam(request);
    LogMonitoring.dumpTailOfLogs(out, tailKb);
    
    out.flush();
  }
  

  private void dumpRIT(HMaster master, PrintWriter out) {
    NavigableMap<String, RegionState> regionsInTransition =
      master.getAssignmentManager().getRegionsInTransition();
    for (Map.Entry<String, RegionState> e : regionsInTransition.entrySet()) {
      String rid = e.getKey();
      RegionState rs = e.getValue();
      out.println("Region " + rid + ": " + rs.toDescriptiveString());
    }
  }

  private void dumpServers(HMaster master, PrintWriter out) {
    Map<ServerName, HServerLoad> servers =
      master.getServerManager().getOnlineServers();
    for (Map.Entry<ServerName, HServerLoad> e : servers.entrySet()) {
      out.println(e.getKey() + ": " + e.getValue());
    }
  }
}
