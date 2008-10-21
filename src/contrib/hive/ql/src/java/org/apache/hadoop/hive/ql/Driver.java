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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import org.antlr.runtime.tree.CommonTree;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.ByteStream;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog("hive.ql.Driver");
  static final private int MAX_ROWS   = 100;
  ByteStream.Output bos = new ByteStream.Output();
  
  private ParseDriver  pd;
  private HiveConf     conf;
  private DataInput    resStream;
  private LogHelper    console;
  private Context      ctx;
  private BaseSemanticAnalyzer sem;
  
  public int countJobs(List<Task<? extends Serializable>> tasks) {
    if (tasks == null)
      return 0;
    int jobs = 0;
    for (Task<? extends Serializable> task: tasks) {
      if (task.isMapRedTask()) {
        jobs++;
      }
      jobs += countJobs(task.getChildTasks());
    }
    return jobs;
  }

  public boolean hasReduceTasks(List<Task<? extends Serializable>> tasks) {
    if (tasks == null)
      return false;

    boolean hasReduce = false;
    for (Task<? extends Serializable> task: tasks) {
      if (task.hasReduce()) {
        return true;
      }

      hasReduce = (hasReduce || hasReduceTasks(task.getChildTasks()));
    }
    return hasReduce;
  }


  /**
   * for backwards compatibility with current tests
   */ 
  public Driver(HiveConf conf) {
    console = new LogHelper(LOG);
    this.conf = conf;
    ctx = new Context(conf);
  }

  public Driver() {
    console = new LogHelper(LOG);
    if(SessionState.get() != null) {
      conf = SessionState.get().getConf();
      ctx = new Context(conf);
    }
  }

  public int run(String command) {

    boolean noName = StringUtils.isEmpty(conf.getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
    int jobs = 0;

    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, command);

    try {
      
      TaskFactory.resetId();
      LOG.info("Starting command: " + command);

      ctx.clear();
      ctx.makeScratchDir();
      resStream = null;
      
      pd = new ParseDriver();
      CommonTree tree = pd.parse(command);

      while((tree.getToken() == null) && (tree.getChildCount() > 0)) {
        tree = (CommonTree)tree.getChild(0);
      }

      sem = SemanticAnalyzerFactory.get(conf, tree);

      // Do semantic analysis and plan generation
      sem.analyze(tree, ctx);
      LOG.info("Semantic Analysis Completed");

      jobs = countJobs(sem.getRootTasks());
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      
      boolean hasReduce = hasReduceTasks(sem.getRootTasks());
      if (hasReduce) {
        console.printInfo("Number of reducers = " + conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS));
        console.printInfo("In order to change numer of reducers use:");
        console.printInfo("  set mapred.reduce.tasks = <number>");
      }

      String jobname = Utilities.abbreviate(command, maxlen - 6);
      int curJob = 0;
      for(Task<? extends Serializable> rootTask: sem.getRootTasks()) {
        // assumption that only top level tasks are map-reduce tasks
        if (rootTask.isMapRedTask()) {
          curJob ++;
          if(noName) {
            conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" + curJob + "/" + jobs + ")");
          }
        }
        rootTask.initialize(conf);
      }

      // A very simple runtime that keeps putting runnable takss
      // on a list and when a job completes, it puts the children at the back of the list
      // while taking the job to run from the front of the list
      Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();

      for(Task<? extends Serializable> rootTask:sem.getRootTasks()) {
        if (runnable.offer(rootTask) == false) {
          LOG.error("Could not insert the first task into the queue");
          return (1);
        }
      }

      while(runnable.peek() != null) {
        Task<? extends Serializable> tsk = runnable.remove();

        int exitVal = tsk.execute();
        if (exitVal != 0) {
          console.printError("FAILED: Execution Error, return code " + exitVal + " from " + tsk.getClass().getName());
          return 9;
        }

        tsk.setDone();

        if (tsk.getChildTasks() == null) {
          continue;
        }

        for(Task<? extends Serializable> child: tsk.getChildTasks()) {
          // Check if the child is runnable
          if (!child.isRunnable()) {
            continue;
          }

          if (runnable.offer(child) == false) {
            LOG.error("Could not add child task to queue");
          }
        }
      }
    } catch (SemanticException e) {
      console.printError("FAILED: Error in semantic analysis: " + e.getMessage(), "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    } catch (ParseException e) {
      console.printError("FAILED: Parse Error: " + e.getMessage(), "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (11);
    } catch (Exception e) {
      // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
      console.printError("FAILED: Unknown exception : " + e.getMessage(),
                         "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (12);
    } finally {
      if(noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, "");
      } 
    }

    console.printInfo("OK");
    return (0);
  }
  
  
  public boolean getResults(Vector<String> res) 
  {
  	if (sem.getFetchTask() != null) {
      if (!sem.getFetchTaskInit()) {
        sem.setFetchTaskInit(true);
        sem.getFetchTask().initialize(conf);
      }
  		boolean ret = sem.getFetchTask().fetch(res);
  		return ret;  		
  	}

    if (resStream == null)
      resStream = ctx.getStream();
    if (resStream == null) return false;
    
    int numRows = 0;
    String row = null;

    while (numRows < MAX_ROWS)
    {
      if (resStream == null) 
      {
        if (numRows > 0)
          return true;
        else
          return false;
      }

      bos.reset();
      Utilities.streamStatus ss;
      try
      {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0)
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        else if (ss == Utilities.streamStatus.TERMINATED)
          row = new String();

        if (row != null) {
          numRows++;
          res.add(row);
        }
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        res = null;
        return false;
      }

      if (ss == Utilities.streamStatus.EOF) 
        resStream = ctx.getStream();
    }
    return true;
  }

  public int close() {
    try {
      // Delete the scratch directory from the context
      ctx.removeScratchDir();
      ctx.clear();
    }
    catch (Exception e) {
      console.printError("FAILED: Unknown exception : " + e.getMessage(),
                         "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return(13);
    }
    
    return(0);
  }
}

