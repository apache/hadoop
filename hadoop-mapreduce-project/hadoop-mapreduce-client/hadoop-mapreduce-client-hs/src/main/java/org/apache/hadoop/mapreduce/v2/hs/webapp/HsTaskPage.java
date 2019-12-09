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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Collection;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TFOOT;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * A Page the shows the status of a given task
 */
public class HsTaskPage extends HsView {

  /**
   * A Block of HTML that will render a given task attempt. 
   */
  static class AttemptsBlock extends HtmlBlock {
    final App app;
    final Configuration conf;

    @Inject
    AttemptsBlock(App ctx, Configuration conf) {
      app = ctx;
      this.conf = conf;
    }

    @Override
    protected void render(Block html) {
      if (!isValidRequest()) {
        html.
          h2($(TITLE));
        return;
      }
      TaskType type = null;
      String symbol = $(TASK_TYPE);
      if (!symbol.isEmpty()) {
        type = MRApps.taskType(symbol);
      } else {
        type = app.getTask().getType();
      }
      
      TR<THEAD<TABLE<Hamlet>>> headRow = html.
      table("#attempts").
        thead().
          tr();
      
      headRow.
            th(".id", "Attempt").
            th(".state", "State").
            th(".status", "Status").
            th(".node", "Node").
            th(".logs", "Logs").
            th(".tsh", "Start Time");
      
      if(type == TaskType.REDUCE) {
        headRow.th("Shuffle Finish Time");
        headRow.th("Merge Finish Time");
      }
      
      headRow.th("Finish Time"); //Attempt
      
      if(type == TaskType.REDUCE) {
        headRow.th("Elapsed Time Shuffle"); //Attempt
        headRow.th("Elapsed Time Merge"); //Attempt
        headRow.th("Elapsed Time Reduce"); //Attempt
      }
      headRow.th("Elapsed Time").
              th(".note", "Note");
      
       TBODY<TABLE<Hamlet>> tbody = headRow.__().__().tbody();
       // Write all the data into a JavaScript array of arrays for JQuery
       // DataTables to display
       StringBuilder attemptsTableData = new StringBuilder("[\n");

       for (TaskAttempt attempt : getTaskAttempts()) {
        final TaskAttemptInfo ta = new MapTaskAttemptInfo(attempt, false);
        String taid = ta.getId();

        String nodeHttpAddr = ta.getNode();
        String containerIdString = ta.getAssignedContainerIdStr();
        String nodeIdString = attempt.getAssignedContainerMgrAddress();
        String nodeRackName = ta.getRack();

        long attemptStartTime = ta.getStartTime();
        long shuffleFinishTime = -1;
        long sortFinishTime = -1;
        long attemptFinishTime = ta.getFinishTime();
        long elapsedShuffleTime = -1;
        long elapsedSortTime = -1;
        long elapsedReduceTime = -1;
        if(type == TaskType.REDUCE) {
          shuffleFinishTime = attempt.getShuffleFinishTime();
          sortFinishTime = attempt.getSortFinishTime();
          elapsedShuffleTime =
              Times.elapsed(attemptStartTime, shuffleFinishTime, false);
          elapsedSortTime =
              Times.elapsed(shuffleFinishTime, sortFinishTime, false);
          elapsedReduceTime =
              Times.elapsed(sortFinishTime, attemptFinishTime, false); 
        }
        long attemptElapsed =
            Times.elapsed(attemptStartTime, attemptFinishTime, false);
        TaskId taskId = attempt.getID().getTaskId();

        attemptsTableData.append("[\"")
        .append(getAttemptId(taskId, ta)).append("\",\"")
        .append(ta.getState()).append("\",\"")
        .append(StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(ta.getStatus()))).append("\",\"")

        .append("<a class='nodelink' href='" + MRWebAppUtil.getYARNWebappScheme() + nodeHttpAddr + "'>")
        .append(nodeRackName + "/" + nodeHttpAddr + "</a>\",\"");

         String logsUrl = url("logs", nodeIdString, containerIdString, taid,
             app.getJob().getUserName());
         if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
             YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
           logsUrl =
               url(MRWebAppUtil.getYARNWebappScheme(), nodeHttpAddr, "node",
                   "containerlogs", containerIdString,
                   app.getJob().getUserName());
         }
         attemptsTableData.append("<a class='logslink' href='").append(logsUrl)
             .append("'>logs</a>\",\"");

        attemptsTableData.append(attemptStartTime).append("\",\"");

        if(type == TaskType.REDUCE) {
          attemptsTableData.append(shuffleFinishTime).append("\",\"")
          .append(sortFinishTime).append("\",\"");
        }
        attemptsTableData.append(attemptFinishTime).append("\",\"");

        if(type == TaskType.REDUCE) {
          attemptsTableData.append(elapsedShuffleTime).append("\",\"")
          .append(elapsedSortTime).append("\",\"")
          .append(elapsedReduceTime).append("\",\"");
        }
          attemptsTableData.append(attemptElapsed).append("\",\"")
          .append(StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(ta.getNote())))
          .append("\"],\n");
      }
       //Remove the last comma and close off the array of arrays
       if(attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
         attemptsTableData.delete(attemptsTableData.length()-2, attemptsTableData.length()-1);
       }
       attemptsTableData.append("]");
       html.script().$type("text/javascript").
           __("var attemptsTableData=" + attemptsTableData).__();

      TR<TFOOT<TABLE<Hamlet>>> footRow = tbody.__().tfoot().tr();
      footRow.
          th().input("search_init").$type(InputType.text).
              $name("attempt_name").$value("Attempt").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_state").$value("State").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_status").$value("Status").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_node").$value("Node").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_node").$value("Logs").__().__().
          th().input("search_init").$type(InputType.text).
              $name("attempt_start_time").$value("Start Time").__().__();
      
      if(type == TaskType.REDUCE) {
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("shuffle_time").$value("Shuffle Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("merge_time").$value("Merge Time").__().__();
      }
      
      footRow.
        th().input("search_init").$type(InputType.text).
            $name("attempt_finish").$value("Finish Time").__().__();
      
      if(type == TaskType.REDUCE) {
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_shuffle_time").$value("Elapsed Shuffle Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_merge_time").$value("Elapsed Merge Time").__().__();
        footRow.
        th().input("search_init").$type(InputType.text).
            $name("elapsed_reduce_time").$value("Elapsed Reduce Time").__().__();
      }

      footRow.
        th().input("search_init").$type(InputType.text).
            $name("attempt_elapsed").$value("Elapsed Time").__().__().
        th().input("search_init").$type(InputType.text).
            $name("note").$value("Note").__().__();
      
      footRow.__().__().__();
    }

    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return ta.getId();
    }

    /**
     * @return true if this is a valid request else false.
     */
    protected boolean isValidRequest() {
      return app.getTask() != null;
    }

    /**
     * @return all of the attempts to render.
     */
    protected Collection<TaskAttempt> getTaskAttempts() {
      return app.getTask().getAttempts().values();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    //override the nav config from commonPReHead
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
    //Set up the java script and CSS for the attempts table
    set(DATATABLES_ID, "attempts");
    set(initID(DATATABLES, "attempts"), attemptsTableInit());
    set(postInitID(DATATABLES, "attempts"), attemptsPostTableInit());
    setTableStyles(html, "attempts");
  }

  /**
   * The content of this page is the attempts block
   * @return AttemptsBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return AttemptsBlock.class;
  }

  /**
   * @return The end of the JS map that is the jquery datatable config for the
   * attempts table. 
   */
  private String attemptsTableInit() {
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    } else {
      TaskId taskID = MRApps.toTaskID($(TASK_ID));
      type = taskID.getTaskType();
    }
    StringBuilder b = tableInit()
      .append(", 'aaData': attemptsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")
      .append("\n,aoColumnDefs:[\n")

      //logs column should not filterable (it includes container ID which may pollute searches)
      .append("\n{'aTargets': [ 4 ]")
      .append(", 'bSearchable': false }")

      .append("\n, {'sType':'natural', 'aTargets': [ 0 ]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [ 5, 6")
      //Column numbers are different for maps and reduces
      .append(type == TaskType.REDUCE ? ", 7, 8" : "")
      .append(" ], 'mRender': renderHadoopDate }")

      .append("\n, {'sType':'numeric', 'aTargets': [")
      .append(type == TaskType.REDUCE ? "9, 10, 11, 12" : "7")
      .append(" ], 'mRender': renderHadoopElapsedTime }]")

      // Sort by id upon page load
      .append("\n, aaSorting: [[0, 'asc']]")
      .append("}");
      return b.toString();
  }

  private String attemptsPostTableInit() {
    return "var asInitVals = new Array();\n" +
           "$('tfoot input').keyup( function () \n{"+
           "  attemptsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
           "} );\n"+
           "$('tfoot input').each( function (i) {\n"+
           "  asInitVals[i] = this.value;\n"+
           "} );\n"+
           "$('tfoot input').focus( function () {\n"+
           "  if ( this.className == 'search_init' )\n"+
           "  {\n"+
           "    this.className = '';\n"+
           "    this.value = '';\n"+
           "  }\n"+
           "} );\n"+
           "$('tfoot input').blur( function (i) {\n"+
           "  if ( this.value == '' )\n"+
           "  {\n"+
           "    this.className = 'search_init';\n"+
           "    this.value = asInitVals[$('tfoot input').index(this)];\n"+
           "  }\n"+
           "} );\n";
  }
}
