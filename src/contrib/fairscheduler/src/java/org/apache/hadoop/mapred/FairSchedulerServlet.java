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

import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at
 * [job tracker URL]/scheduler when the {@link FairScheduler} is in use.
 * 
 * The main features are viewing each job's task count and fair share, ability
 * to change job priorities and pools from the UI, and ability to switch the
 * scheduler to FIFO mode without restarting the JobTracker if this is required
 * for any reason.
 * 
 * There is also an "advanced" view for debugging that can be turned on by
 * going to [job tracker URL]/scheduler?advanced.
 */
public class FairSchedulerServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;
  private static final DateFormat DATE_FORMAT = 
    new SimpleDateFormat("MMM dd, HH:mm");
  
  private FairScheduler scheduler;
  private JobTracker jobTracker;
  private static long lastId = 0; // Used to generate unique element IDs

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (FairScheduler) servletContext.getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
  }
  
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    // If the request has a set* param, handle that and redirect to the regular
    // view page so that the user won't resubmit the data if they hit refresh.
    boolean advancedView = request.getParameter("advanced") != null;
    if (request.getParameter("setFifo") != null) {
      scheduler.setUseFifo(request.getParameter("setFifo").equals("true"));
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPool") != null) {
      Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
      PoolManager poolMgr = null;
      synchronized (scheduler) {
        poolMgr = scheduler.getPoolManager();
      }
      String pool = request.getParameter("setPool");
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          synchronized(scheduler){
            poolMgr.setPool(job, pool);
          }
          scheduler.update();
          break;
        }
      }      
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPriority") != null) {
      Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();      
      JobPriority priority = JobPriority.valueOf(request.getParameter(
          "setPriority"));
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          job.setPriority(priority);
          scheduler.update();
          break;
        }
      }      
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    // Print out the normal response
    response.setContentType("text/html");
    PrintWriter out = new PrintWriter(response.getOutputStream());
    String hostname = StringUtils.simpleHostname(
        jobTracker.getJobTrackerMachine());
    out.print("<html><head>");
    out.printf("<title>%s Job Scheduler Admininstration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" " + 
        "href=\"/static/hadoop.css\">\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " + 
        "Job Scheduler Administration</h1>\n", hostname);
    showPools(out, advancedView);
    showJobs(out, advancedView);
    showAdminForm(out, advancedView);
    out.print("</body></html>\n");
    out.close();
  }

  /**
   * Print a view of pools to the given output writer.
   */
  private void showPools(PrintWriter out, boolean advancedView) {
    synchronized(scheduler) {
      PoolManager poolManager = scheduler.getPoolManager();
      out.print("<h2>Pools</h2>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><th>Pool</th><th>Running Jobs</th>" + 
          "<th>Min Maps</th><th>Min Reduces</th>" + 
          "<th>Running Maps</th><th>Running Reduces</th></tr>\n");
      List<Pool> pools = new ArrayList<Pool>(poolManager.getPools());
      Collections.sort(pools, new Comparator<Pool>() {
        public int compare(Pool p1, Pool p2) {
          if (p1.isDefaultPool())
            return 1;
          else if (p2.isDefaultPool())
            return -1;
          else return p1.getName().compareTo(p2.getName());
        }});
      for (Pool pool: pools) {
        int runningMaps = 0;
        int runningReduces = 0;
        for (JobInProgress job: pool.getJobs()) {
          JobInfo info = scheduler.infos.get(job);
          if (info != null) {
            runningMaps += info.runningMaps;
            runningReduces += info.runningReduces;
          }
        }
        out.print("<tr>\n");
        out.printf("<td>%s</td>\n", pool.getName());
        out.printf("<td>%s</td>\n", pool.getJobs().size());
        out.printf("<td>%s</td>\n", poolManager.getAllocation(pool.getName(),
            TaskType.MAP));
        out.printf("<td>%s</td>\n", poolManager.getAllocation(pool.getName(), 
            TaskType.REDUCE));
        out.printf("<td>%s</td>\n", runningMaps);
        out.printf("<td>%s</td>\n", runningReduces);
        out.print("</tr>\n");
      }
      out.print("</table>\n");
    }
  }

  /**
   * Print a view of running jobs to the given output writer.
   */
  private void showJobs(PrintWriter out, boolean advancedView) {
    out.print("<h2>Running Jobs</h2>\n");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    int colsPerTaskType = advancedView ? 6 : 3;
    out.printf("<tr><th rowspan=2>Submitted</th>" + 
        "<th rowspan=2>JobID</th>" +
        "<th rowspan=2>User</th>" +
        "<th rowspan=2>Name</th>" +
        "<th rowspan=2>Pool</th>" +
        "<th rowspan=2>Priority</th>" +
        "<th colspan=%d>Maps</th>" +
        "<th colspan=%d>Reduces</th>",
        colsPerTaskType, colsPerTaskType);
    out.print("</tr><tr>\n");
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th><th>Deficit</th><th>minMaps</th>" : ""));
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th><th>Deficit</th><th>minReduces</th>" : ""));
    out.print("</tr>\n");
    Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
    synchronized (scheduler) {
      for (JobInProgress job: runningJobs) {
        JobProfile profile = job.getProfile();
        JobInfo info = scheduler.infos.get(job);
        if (info == null) { // Job finished, but let's show 0's for info
          info = new JobInfo();
        }
        out.print("<tr>\n");
        out.printf("<td>%s</td>\n", DATE_FORMAT.format(
            new Date(job.getStartTime())));
        out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
            profile.getJobID(), profile.getJobID());
        out.printf("<td>%s</td>\n", profile.getUser());
        out.printf("<td>%s</td>\n", profile.getJobName());
        out.printf("<td>%s</td>\n", generateSelect(
            scheduler.getPoolManager().getPoolNames(),
            scheduler.getPoolManager().getPoolName(job),
            "/scheduler?setPool=<CHOICE>&jobid=" + profile.getJobID() +
            (advancedView ? "&advanced" : "")));
        out.printf("<td>%s</td>\n", generateSelect(
            Arrays.asList(new String[]
                {"VERY_LOW", "LOW", "NORMAL", "HIGH", "VERY_HIGH"}),
            job.getPriority().toString(),
            "/scheduler?setPriority=<CHOICE>&jobid=" + profile.getJobID() +
            (advancedView ? "&advanced" : "")));
        out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
            job.finishedMaps(), job.desiredMaps(), info.runningMaps,
            info.mapFairShare);
        if (advancedView) {
          out.printf("<td>%8.1f</td>\n", info.mapWeight);
          out.printf("<td>%s</td>\n", info.neededMaps > 0 ?
              (info.mapDeficit / 1000) + "s" : "--");
          out.printf("<td>%d</td>\n", info.minMaps);
        }
        out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
            job.finishedReduces(), job.desiredReduces(), info.runningReduces,
            info.reduceFairShare);
        if (advancedView) {
          out.printf("<td>%8.1f</td>\n", info.reduceWeight);
          out.printf("<td>%s</td>\n", info.neededReduces > 0 ?
              (info.reduceDeficit / 1000) + "s" : "--");
          out.printf("<td>%d</td>\n", info.minReduces);
        }
        out.print("</tr>\n");
      }
    }
    out.print("</table>\n");
  }

  /**
   * Generate a HTML select control with a given list of choices and a given
   * option selected. When the selection is changed, take the user to the
   * <code>submitUrl</code>. The <code>submitUrl</code> can be made to include
   * the option selected -- the first occurrence of the substring
   * <code>&lt;CHOICE&gt;</code> will be replaced by the option chosen.
   */
  private String generateSelect(Iterable<String> choices, 
      String selectedChoice, String submitUrl) {
    StringBuilder html = new StringBuilder();
    String id = "select" + lastId++;
    html.append("<select id=\"" + id + "\" name=\"" + id + "\" " + 
        "onchange=\"window.location = '" + submitUrl + 
        "'.replace('<CHOICE>', document.getElementById('" + id +
        "').value);\">\n");
    for (String choice: choices) {
      html.append(String.format("<option value=\"%s\"%s>%s</option>\n",
          choice, (choice.equals(selectedChoice) ? " selected" : ""), choice));
    }
    html.append("</select>\n");
    return html.toString();
  }

  /**
   * Print the administration form at the bottom of the page, which currently
   * only includes the button for switching between FIFO and Fair Scheduling.
   */
  private void showAdminForm(PrintWriter out, boolean advancedView) {
    out.print("<h2>Scheduling Mode</h2>\n");
    String curMode = scheduler.getUseFifo() ? "FIFO" : "Fair Sharing";
    String otherMode = scheduler.getUseFifo() ? "Fair Sharing" : "FIFO";
    String advParam = advancedView ? "?advanced" : "";
    out.printf("<form method=\"post\" action=\"/scheduler%s\">\n", advParam);
    out.printf("<p>The scheduler is currently using <b>%s mode</b>. " +
        "<input type=\"submit\" value=\"Switch to %s mode.\" " + 
        "onclick=\"return confirm('Are you sure you want to change " +
        "scheduling mode to %s?')\" />\n",
        curMode, otherMode, otherMode);
    out.printf("<input type=\"hidden\" name=\"setFifo\" value=\"%s\" />",
        !scheduler.getUseFifo());
    out.print("</form>\n");
  }
}
