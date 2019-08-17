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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Render all of the jobs that the history server is aware of.
 */
public class HsJobsBlock extends HtmlBlock {
  final AppContext appContext;
  final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");
  private UserGroupInformation ugi;
  private boolean isFilterAppListByUserEnabled;
  private boolean areAclsEnabled;
  private AccessControlList adminAclList;

  @Inject
  HsJobsBlock(Configuration conf, AppContext appCtx, ViewContext ctx) {
    super(ctx);
    appContext = appCtx;
    isFilterAppListByUserEnabled = conf
        .getBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, false);
    areAclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    adminAclList = new AccessControlList(conf.get(MRConfig.MR_ADMINS, " "));
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      h2("Retired Jobs").
      table("#jobs").
        thead().
          tr().
            th("Submit Time").
            th("Start Time").
            th("Finish Time").
            th(".id", "Job ID").
            th(".name", "Name").
            th("User").
            th("Queue").
            th(".state", "State").
            th("Maps Total").
            th("Maps Completed").
            th("Reduces Total").
            th("Reduces Completed").
            th("Elapsed Time").__().__().
        tbody();
    LOG.info("Getting list of all Jobs.");
    // Write all the data into a JavaScript array of arrays for JQuery
    // DataTables to display
    StringBuilder jobsTableData = new StringBuilder("[\n");
    for (Job j : appContext.getAllJobs().values()) {
      JobInfo job = new JobInfo(j);
      ugi = getCallerUGI();
      // Allow to list only per-user apps if incoming ugi has permission.
      if (isFilterAppListByUserEnabled && ugi != null
          && !checkAccess(job.getUserName())) {
        continue;
      }
      jobsTableData.append("[\"")
      .append(dateFormat.format(new Date(job.getSubmitTime()))).append("\",\"")
      .append(job.getFormattedStartTimeStr(dateFormat)).append("\",\"")
      .append(dateFormat.format(new Date(job.getFinishTime()))).append("\",\"")
      .append("<a href='").append(url("job", job.getId())).append("'>")
      .append(job.getId()).append("</a>\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getName()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getUserName()))).append("\",\"")
      .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
        job.getQueueName()))).append("\",\"")
      .append(job.getState()).append("\",\"")
      .append(String.valueOf(job.getMapsTotal())).append("\",\"")
      .append(String.valueOf(job.getMapsCompleted())).append("\",\"")
      .append(String.valueOf(job.getReducesTotal())).append("\",\"")
      .append(String.valueOf(job.getReducesCompleted())).append("\",\"")
          .append(
              StringUtils.formatTimeSortable(Times.elapsed(job.getStartTime(),
                  job.getFinishTime(), false))).append("\"],\n");
    }

    //Remove the last comma and close off the array of arrays
    if(jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
      jobsTableData.delete(jobsTableData.length()-2, jobsTableData.length()-1);
    }
    jobsTableData.append("]");
    html.script().$type("text/javascript").
        __("var jobsTableData=" + jobsTableData).__();
    tbody.__().
    tfoot().
      tr().
        th().input("search_init").$type(InputType.text)
          .$name("submit_time").$value("Submit Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("start_time").$value("Start Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("finish_time").$value("Finish Time").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("job_id").$value("Job ID").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("name").$value("Name").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("user").$value("User").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("queue").$value("Queue").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("state").$value("State").__().__().
        th().input("search_init").$type(InputType.text)
          .$name("maps_total").$value("Maps Total").__().__().
        th().input("search_init").$type(InputType.text).
          $name("maps_completed").$value("Maps Completed").__().__().
        th().input("search_init").$type(InputType.text).
          $name("reduces_total").$value("Reduces Total").__().__().
        th().input("search_init").$type(InputType.text).
          $name("reduces_completed").$value("Reduces Completed").__().__().
        th().input("search_init").$type(InputType.text).
          $name("elapsed_time").$value("Elapsed Time").__().__().
        __().
        __().
        __();
  }

  private boolean checkAccess(String userName) {
    if(!areAclsEnabled) {
      return true;
    }

    // User could see its own job.
    if (ugi.getShortUserName().equals(userName)) {
      return true;
    }

    // Admin could also see all jobs
    if (adminAclList != null && adminAclList.isUserAllowed(ugi)) {
      return true;
    }
    return false;
  }
}
