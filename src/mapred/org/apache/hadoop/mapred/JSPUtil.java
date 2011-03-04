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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

class JSPUtil {
  private static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";
  
  public static final Configuration conf = new Configuration();

  //LRU based cache
  private static final Map<String, JobInfo> jobHistoryCache = 
    new LinkedHashMap<String, JobInfo>(); 

  private static final int CACHE_SIZE = 
    conf.getInt("mapred.job.tracker.jobhistory.lru.cache.size", 5);

  private static final Log LOG = LogFactory.getLog(JSPUtil.class);
  /**
   * Method used to process the request from the job page based on the 
   * request which it has received. For example like changing priority.
   * 
   * @param request HTTP request Object.
   * @param response HTTP response object.
   * @param tracker {@link JobTracker} instance
   * @throws IOException
   */
  public static void processButtons(HttpServletRequest request,
      HttpServletResponse response, JobTracker tracker) throws IOException {

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false)
        && request.getParameter("killJobs") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");
      if (jobs != null) {
        for (String job : jobs) {
          tracker.killJob(JobID.forName(job));
        }
      }
    }

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false) && 
          request.getParameter("changeJobPriority") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");

      if (jobs != null) {
        JobPriority jobPri = JobPriority.valueOf(request
            .getParameter("setJobPriority"));

        for (String job : jobs) {
          tracker.setJobPriority(JobID.forName(job), jobPri);
        }
      }
    }
  }

  /**
   * Method used to generate the Job table for Job pages.
   * 
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @return
   * @throws IOException
   */
  public static String generateJobTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId) throws IOException {

    boolean isModifiable = label.equals("Running") 
                                && conf.getBoolean(
                                      PRIVATE_ACTIONS_KEY, false);
    StringBuffer sb = new StringBuffer();
    
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    if (jobs.size() > 0) {
      if (isModifiable) {
        sb.append("<form action=\"/jobtracker.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
        sb.append("<tr>");
        sb.append("<td><input type=\"Button\" onclick=\"selectAll()\" " +
        		"value=\"Select All\" id=\"checkEm\"></td>");
        sb.append("<td>");
        sb.append("<input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\">");
        sb.append("</td");
        sb.append("<td><nobr>");
        sb.append("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
          sb.append("<option"
              + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">")
              + prio + "</option>");
        }

        sb.append("</select>");
        sb.append("<input type=\"submit\" name=\"changeJobPriority\" " +
        		"value=\"Change\">");
        sb.append("</nobr></td>");
        sb.append("<td colspan=\"10\">&nbsp;</td>");
        sb.append("</tr>");
        sb.append("<td>&nbsp;</td>");
      } else {
        sb.append("<tr>");
      }

      sb.append("<td><b>Jobid</b></td><td><b>Priority" +
      		"</b></td><td><b>User</b></td>");
      sb.append("<td><b>Name</b></td>");
      sb.append("<td><b>Map % Complete</b></td>");
      sb.append("<td><b>Map Total</b></td>");
      sb.append("<td><b>Maps Completed</b></td>");
      sb.append("<td><b>Reduce % Complete</b></td>");
      sb.append("<td><b>Reduce Total</b></td>");
      sb.append("<td><b>Reduces Completed</b></td>");
      sb.append("<td><b>Job Scheduling Information</b></td>");
      sb.append("</tr>\n");
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String name = profile.getJobName();
        String jobpri = job.getPriority().toString();
        String schedulingInfo = job.getStatus().getSchedulingInfo();

        if (isModifiable) {
          sb.append("<tr><td><input TYPE=\"checkbox\" " +
          		"onclick=\"checkButtonVerbage()\" " +
          		"name=\"jobCheckBox\" value="
                  + jobid + "></td>");
        } else {
          sb.append("<tr>");
        }

        sb.append("<td id=\"job_" + rowId
            + "\"><a href=\"jobdetails.jsp?jobid=" + jobid + "&refresh="
            + refresh + "\">" + jobid + "</a></td>" + "<td id=\"priority_"
            + rowId + "\">" + jobpri + "</td>" + "<td id=\"user_" + rowId
            + "\">" + profile.getUser() + "</td>" + "<td id=\"name_" + rowId
            + "\">" + ("".equals(name) ? "&nbsp;" : name) + "</td>" + "<td>"
            + StringUtils.formatPercent(status.mapProgress(), 2)
            + ServletUtil.percentageGraph(status.mapProgress() * 100, 80)
            + "</td><td>" + desiredMaps + "</td><td>" + completedMaps
            + "</td><td>"
            + StringUtils.formatPercent(status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(status.reduceProgress() * 100, 80)
            + "</td><td>" + desiredReduces + "</td><td> " + completedReduces 
            + "</td><td>" + schedulingInfo
            + "</td></tr>\n");
      }
      if (isModifiable) {
        sb.append("</form>\n");
      }
    } else {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      		"</td></tr>\n");
    }
    sb.append("</table>\n");
    
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  public static String generateRetiredJobTable(JobTracker tracker, int rowId) 
    throws IOException {

    StringBuffer sb = new StringBuffer();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    Iterator<RetireJobInfo> iterator = 
      tracker.retireJobs.getAll().descendingIterator();
    if (!iterator.hasNext()) {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      "</td></tr>\n");
    } else {
      sb.append("<tr>");
      
      sb.append("<td><b>Jobid</b></td>");
      sb.append("<td><b>Priority</b></td>");
      sb.append("<td><b>User</b></td>");
      sb.append("<td><b>Name</b></td>");
      sb.append("<td><b>State</b></td>");
      sb.append("<td><b>Start Time</b></td>");
      sb.append("<td><b>Finish Time</b></td>");
      sb.append("<td><b>Map % Complete</b></td>");
      sb.append("<td><b>Reduce % Complete</b></td>");
      sb.append("<td><b>Job Scheduling Information</b></td>");
      sb.append("</tr>\n");
      for (int i = 0; i < 100 && iterator.hasNext(); i++) {
        RetireJobInfo info = iterator.next();
        String historyFile = info.getHistoryFile();
        String historyFileUrl = null;
        if (historyFile != null && !historyFile.equals("")) {
          try {
            historyFileUrl = URLEncoder.encode(info.getHistoryFile(), "UTF-8");
          } catch (UnsupportedEncodingException e) {
            LOG.warn("Can't create history url ", e);
          }
        }
        sb.append("<tr>");
        sb.append(
            "<td id=\"job_" + rowId + "\">" + 
            
              (historyFileUrl == null ? "" :
              "<a href=\"jobdetailshistory.jsp?jobid=" + 
              info.status.getJobId() + "&logFile=" + historyFileUrl + "\">") + 
              
              info.status.getJobId() + "</a></td>" +
            
            "<td id=\"priority_" + rowId + "\">" + 
              info.status.getJobPriority().toString() + "</td>" +
            "<td id=\"user_" + rowId + "\">" + info.profile.getUser() 
              + "</td>" +
            "<td id=\"name_" + rowId + "\">" + info.profile.getJobName() 
              + "</td>" +
            "<td>" + JobStatus.getJobRunState(info.status.getRunState()) 
              + "</td>" +
            "<td>" + new Date(info.status.getStartTime()) + "</td>" +
            "<td>" + new Date(info.finishTime) + "</td>" +
            
            "<td>" + StringUtils.formatPercent(info.status.mapProgress(), 2)
            + ServletUtil.percentageGraph(info.status.mapProgress() * 100, 80) + 
              "</td>" +
            
            "<td>" + StringUtils.formatPercent(info.status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(
               info.status.reduceProgress() * 100, 80) + 
              "</td>" +
            
            "<td>" + info.status.getSchedulingInfo() + "</td>" +
            
            "</tr>\n");
        rowId++;
      }
    }
    sb.append("</table>\n");
    return sb.toString();
  }

  static JobInfo getJobInfo(HttpServletRequest request, FileSystem fs) 
      throws IOException {
    String jobid = request.getParameter("jobid");
    String logFile = request.getParameter("logFile");
    synchronized(jobHistoryCache) {
      JobInfo jobInfo = jobHistoryCache.remove(jobid);
      if (jobInfo == null) {
        jobInfo = new JobHistory.JobInfo(jobid);
        LOG.info("Loading Job History file "+jobid + ".   Cache size is " +
            jobHistoryCache.size());
        DefaultJobHistoryParser.parseJobTasks( logFile, jobInfo, fs) ; 
      }
      jobHistoryCache.put(jobid, jobInfo);
      if (jobHistoryCache.size() > CACHE_SIZE) {
        Iterator<Map.Entry<String, JobInfo>> it = 
          jobHistoryCache.entrySet().iterator();
        String removeJobId = it.next().getKey();
        it.remove();
        LOG.info("Job History file removed form cache "+removeJobId);
      }
      return jobInfo;
    }
  }
}
