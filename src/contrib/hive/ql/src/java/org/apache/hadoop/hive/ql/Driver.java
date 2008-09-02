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

import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog("hive.ql.Driver");
  static final private int separator  = Utilities.ctrlaCode;
  static final private int terminator = Utilities.newLineCode;
  static final private int MAX_ROWS   = 100;
  
  private ParseDriver pd;
  private HiveConf conf;
  private InputStream resStream;
  private LogHelper console;
  private Context   ctx;
  
  public static int getSeparator() {
    return separator;
  }

  public static int getTerminator() {
    return terminator;
  }
  
  public int countJobs(Collection tasks) {
    if (tasks == null)
      return 0;
    int jobs = 0;
    for (Object task: tasks) {
      if ((task instanceof ExecDriver) || (task instanceof MapRedTask)) {
        jobs++;
      }
      jobs += countJobs(((Task) task).getChildTasks());
    }
    return jobs;
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
      BaseSemanticAnalyzer sem;
      LOG.info("Starting command: " + command);

      if (resStream != null)
      {
        resStream.close();
        resStream = null;
      }
      ctx.clear();

      pd = new ParseDriver();
      CommonTree tree = pd.parse(command);

      while((tree.getToken() == null) && (tree.getChildCount() > 0)) {
        tree = (CommonTree)tree.getChild(0);
      }

      sem = SemanticAnalyzerFactory.get(conf, tree);

      // Do semantic analysis and plan generation
      sem.analyze(tree, ctx);
      LOG.info("Semantic Analysis Completed");
      for(Task rootTask: sem.getRootTasks()) {
        rootTask.initialize(conf);
      }

      jobs = countJobs(sem.getRootTasks());
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }

      // A very simple runtime that keeps putting runnable takss
      // on a list and when a job completes, it puts the children at the back of the list
      // while taking the job to run from the front of the list
      Queue<Task> runnable = new LinkedList<Task>();

      for(Task rootTask:sem.getRootTasks()) {
        if (runnable.offer(rootTask) == false) {
          LOG.error("Could not insert the first task into the queue");
          return (1);
        }
      }

      while(runnable.peek() != null) {
        Task tsk = runnable.remove();

        if(noName) {
          conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, Utilities.abbreviate(command, maxlen));
        }

        int exitVal = tsk.execute();
        if (exitVal != 0) {
          console.printError("FAILED: Execution Error, return code " + exitVal + " from " + tsk.getClass().getName());
          return 9;
        }

        tsk.setDone();

        if (tsk.getChildTasks() == null) {
          continue;
        }

        for(Object child: tsk.getChildTasks()) {
          // Check if the child is runnable
          if (!((Task)child).isRunnable()) {
            continue;
          }

          if (runnable.offer((Task)child) == false) {
            LOG.error("Could not add child task to queue");
          }
        }
      }
    } catch (SemanticException e) {
      console.printError("FAILED: Error in semantic analysis: " + e.getMessage());
      return (10);
    } catch (ParseException e) {
      console.printError("FAILED: Parse Error: " + e.getMessage());
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
    if (jobs > 0) {
      console.printInfo("OK");
    }
    return (0);
  }
  
  
  public boolean getResults(Vector<Vector<String>> res) {

    if (resStream == null)
      resStream = ctx.getStream();
    if (resStream == null) return false;
    
    int sizeArr = 128;
    char[] tmpCharArr = new char[sizeArr];
    
    for (int numRows = 0; numRows < MAX_ROWS; numRows++)
    {
      if (resStream == null) {
        if (numRows > 0) {
          return true;
        }
        else {
          return false;
        }
      }
      boolean eof = false;
      Vector<String> row = new Vector<String>();
      String col;
      int len = 0;
      while (true) {
        char c;
        try {
          int i = resStream.read();
          if (i == -1)
          {
            eof = true;
            break;
          }
          
          c = (char)i;
          
          if (c == terminator) {
            col = new String(tmpCharArr, 0, len);
            len = 0;
            row.add(col.equals(Utilities.nullStringStorage) ? null : col);
            res.add(row);
            break;
          }
          else if (c == separator) {
            col = new String(tmpCharArr, 0, len);
            len = 0;
            row.add(col.equals(Utilities.nullStringStorage) ? null : col);
          }
          else
          {
            if (sizeArr == len)
            {
              char[] tmp = new char[2*sizeArr];
              sizeArr *= 2;
              for (int idx = 0; idx < len; idx++)
                tmp[idx] = tmpCharArr[idx];
              tmpCharArr = tmp;
            }
            tmpCharArr[len++] = c;
          }
          
        } 
        catch (java.io.IOException e) {
          console.printError("FAILED: Unknown exception : " + e.getMessage(),
                             "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
          return false;
        }
      }
      
      if (eof)
      {
        if (len > 0)
        {
          col = new String(tmpCharArr, 0, len);
          len = 0;
          row.add(col.equals(Utilities.nullStringStorage) ? null : col);
          res.add(row);
        }

        resStream = ctx.getStream();
      }
    }
    
    return true;
  }
}

