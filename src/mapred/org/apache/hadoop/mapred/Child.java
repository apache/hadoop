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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;

/** 
 * The main() for child processes. 
 */

class Child {

  public static final Log LOG =
    LogFactory.getLog(Child.class);

  static volatile TaskAttemptID taskid = null;
  static volatile boolean isCleanup;

  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");

    final JobConf defaultConf = new JobConf();
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address = new InetSocketAddress(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    final int SLEEP_LONGER_COUNT = 5;
    int jvmIdInt = Integer.parseInt(args[3]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),firstTaskid.isMap(),jvmIdInt);

    // file name is passed thru env
    String jobTokenFile = 
      System.getenv().get(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    Credentials credentials = 
      TokenCache.loadTokens(jobTokenFile, defaultConf);
    LOG.debug("loading token. # keys =" +credentials.numberOfSecretKeys() + 
        "; from file=" + jobTokenFile);
    
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    jt.setService(new Text(address.getAddress().getHostAddress() + ":"
        + address.getPort()));
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);

    UserGroupInformation taskOwner 
     = UserGroupInformation.createRemoteUser(firstTaskid.getJobID().toString());
    taskOwner.addToken(jt);
    
    // Set the credentials
    defaultConf.setCredentials(credentials);
    
    final TaskUmbilicalProtocol umbilical = 
      taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
              TaskUmbilicalProtocol.versionID,
              address,
              defaultConf);
        }
    });
    
    int numTasksToExecute = -1; //-1 signifies "no limit"
    int numTasksExecuted = 0;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (taskid != null) {
            TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
          }
        } catch (Throwable throwable) {
        }
      }
    });
    Thread t = new Thread() {
      public void run() {
        //every so often wake up and syncLogs so that we can track
        //logs of the currently running task
        while (true) {
          try {
            Thread.sleep(5000);
            if (taskid != null) {
              TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
            }
          } catch (InterruptedException ie) {
          } catch (IOException iee) {
            LOG.error("Error in syncLogs: " + iee);
            System.exit(-1);
          }
        }
      }
    };
    t.setName("Thread for syncLogs");
    t.setDaemon(true);
    t.start();
    
    String pid = "";
    if (!Shell.WINDOWS) {
      pid = System.getenv().get("JVM_PID");
    }
    JvmContext context = new JvmContext(jvmId, pid);
    int idleLoopCount = 0;
    Task task = null;
    
    UserGroupInformation childUGI = null;
    
    try {
      while (true) {
        taskid = null;
        JvmTask myTask = umbilical.getTask(context);
        if (myTask.shouldDie()) {
          break;
        } else {
          if (myTask.getTask() == null) {
            taskid = null;
            if (++idleLoopCount >= SLEEP_LONGER_COUNT) {
              //we sleep for a bigger interval when we don't receive
              //tasks for a while
              Thread.sleep(1500);
            } else {
              Thread.sleep(500);
            }
            continue;
          }
        }
        idleLoopCount = 0;
        task = myTask.getTask();
        taskid = task.getTaskID();
        isCleanup = task.isTaskCleanupTask();
        // reset the statistics for the task
        FileSystem.clearStatistics();

        //create the index file so that the log files 
        //are viewable immediately
        TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
        
        // Create the job-conf and set credentials
        final JobConf job = new JobConf(task.getJobFile());
        job.setCredentials(defaultConf.getCredentials());

        // set the jobTokenFile into task
        task.setJobTokenSecret(JobTokenSecretManager.
            createSecretKey(jt.getPassword()));

        // setup the child's mapred-local-dir. The child is now sandboxed and
        // can only see files down and under attemtdir only.
        TaskRunner.setupChildMapredLocalDirs(task, job);

        //setupWorkDir actually sets up the symlinks for the distributed
        //cache. After a task exits we wipe the workdir clean, and hence
        //the symlinks have to be rebuilt.
        TaskRunner.setupWorkDir(job, new File(".").getAbsoluteFile());

        numTasksToExecute = job.getNumTasksToExecutePerJvm();
        assert(numTasksToExecute != 0);

        task.setConf(job);

        // Initiate Java VM metrics
        JvmMetrics.init(task.getPhase().toString(), job.getSessionId());
        LOG.debug("Creating remote user to execute task: " + job.get("user.name"));
        childUGI = UserGroupInformation.createRemoteUser(job.get("user.name"));
        // Add tokens to new user so that it may execute its task correctly.
        for(Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
          childUGI.addToken(token);
        }
        
        // Create a final reference to the task for the doAs block
        final Task taskFinal = task;
        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            try {
              // use job-specified working directory
              FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
              taskFinal.run(job, umbilical);             // run the task
            } finally {
              TaskLog.syncLogs(firstTaskid, taskid, isCleanup);
            }

            return null;
          }
        });
        if (numTasksToExecute > 0 && ++numTasksExecuted == numTasksToExecute) {
          break;
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskid, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Error running child", exception);
      try {
        if (task != null) {
          // do cleanup for the task
          if(childUGI == null) {
            task.taskCleanup(umbilical);
          } else {
            final Task taskFinal = task;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                taskFinal.taskCleanup(umbilical);
                return null;
              }
            });
          }
        }
      } catch (Exception e) {
        LOG.info("Error cleaning up", e);
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskid != null) {
        umbilical.reportDiagnosticInfo(taskid, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
                + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        Throwable tCause = throwable.getCause();
        String cause = tCause == null 
                       ? throwable.getMessage() 
                       : StringUtils.stringifyException(tCause);
        umbilical.fatalError(taskid, cause);
      }
    } finally {
      RPC.stopProxy(umbilical);
      MetricsContext metricsContext = MetricsUtil.getContext("mapred");
      metricsContext.close();
      // Shutting down log4j of the child-vm... 
      // This assumes that on return from Task.run() 
      // there is no more logging done.
      LogManager.shutdown();
    }
  }
}
