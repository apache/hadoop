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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for controlling queue allocations, installed at
 * [job tracker URL]/scheduler when the {@link DynamicPriorityScheduler} 
 * is in use.
 * operations supported: <br>
 * price <br>
 * time <br>
 * info=queue_to_query (requires user or admin privilege><br>
 * infos (requires admin privilege) <br> 
 * addBudget=budget_to_add,queue=queue_to_change 
 * (requires admin privilege) <br>
 * setSpending=spending_to_set,queue=queue_to_change 
 * (requires user or admin privilege) <br>
 * addQueue=queue_to_add (requires admin privilege) <br>
 * removeQueue=queue_to_remove (requires admin privilege) <br>
 */
public class DynamicPriorityServlet extends HttpServlet {

  private DynamicPriorityScheduler scheduler;
  private JobTracker jobTracker;
  private PriorityAuthorization auth;
  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = getServletContext();
    scheduler = 
        (DynamicPriorityScheduler) servletContext.getAttribute("scheduler");
    jobTracker = (JobTracker) scheduler.taskTrackerManager;
    auth = new PriorityAuthorization();
    auth.init(scheduler.conf);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }

  private void checkAdmin(int role, String query) throws IOException {
    if (role != PriorityAuthorization.ADMIN) {
      throw new IOException("ACCESS DENIED: " + query);
    }
  }
  private void checkUser(int role, HttpServletRequest request, 
      String queue, String query) throws IOException {
    if (role == PriorityAuthorization.ADMIN) {
      return;
    } 
    if (role == PriorityAuthorization.USER &&
        request.getParameter("user").equals(queue)) {
      return;
    }
    throw new IOException("ACCESS DENIED: " + query);
  }

  @Override
  public void doGet(HttpServletRequest request, 
      HttpServletResponse response) throws ServletException, IOException {
    String query = request.getQueryString();
    int role = auth.authorize(query,
        request.getHeader("Authorization"),
        request.getParameter("user"),
        request.getParameter("timestamp"));

    String queue = request.getParameter("queue");
    String info = "";
    // admin
    if (request.getParameter("addQueue") != null) {
      checkAdmin(role, query);
      queue = request.getParameter("addQueue");
      scheduler.allocations.addQueue(queue);
      info = scheduler.allocations.getInfo(queue); 
    }
    // admin
    if (request.getParameter("removeQueue") != null) {
      checkAdmin(role, query);
      queue = request.getParameter("removeQueue");
      scheduler.allocations.removeQueue(queue);
      info = scheduler.allocations.getInfo(queue); 
    }
    // admin
    if (request.getParameter("addBudget") != null) {
      checkAdmin(role, query);
      float budget = Float.parseFloat(request.getParameter("addBudget"));
      scheduler.allocations.addBudget(queue, budget);
      info = scheduler.allocations.getInfo(queue); 
    }
    // user
    if (request.getParameter("setSpending") != null) {
      checkUser(role, request, queue, query);
      float spending = Float.parseFloat(request.getParameter("setSpending"));
      scheduler.allocations.setSpending(queue, spending);
      info = scheduler.allocations.getInfo(queue); 
    }
    // user
    if (request.getParameter("info") != null) {
      queue = request.getParameter("info");
      checkUser(role, request, queue, query);
      info = scheduler.allocations.getQueueInfo(queue);
    }
    // admin
    if (request.getParameter("infos") != null) {
      checkAdmin(role, query);
      info = scheduler.allocations.getQueueInfos();
    }

    // all
    if (request.getParameter("price") != null) {
      info = Float.toString(scheduler.allocations.getPrice());
      info = "<price>" + info + "</price>\n";
    }
    // all
    if (request.getParameter("time") != null) {
      info = "<start>" + Long.toString(PriorityAuthorization.START_TIME) + 
          "</start>\n";
      info += "<time>" + Long.toString(System.currentTimeMillis()) + 
          "</time>\n";
    }
    if (info == null) {
      info = "";
    }
    response.setContentType("text/xml");
    PrintWriter out = new PrintWriter(response.getOutputStream());
    String hostname = StringUtils.simpleHostname(
        jobTracker.getJobTrackerMachine());
    out.print("<QueueInfo>");
    out.printf("<host>%s</host>\n", hostname);
    out.printf("%s", info);
    out.print("</QueueInfo>\n");
    out.close();
  }
}
