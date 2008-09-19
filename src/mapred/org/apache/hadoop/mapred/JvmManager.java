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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

class JvmManager {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.JvmManager");

  JvmManagerForType mapJvmManager;

  JvmManagerForType reduceJvmManager;
  
  TaskTracker tracker;

  public JvmEnv constructJvmEnv(List<String> setup, Vector<String>vargs,
      File stdout,File stderr,long logSize, File workDir, 
      Map<String,String> env, String pidFile, JobConf conf) {
    return new JvmEnv(setup,vargs,stdout,stderr,logSize,workDir,env,pidFile,conf);
  }
  
  public JvmManager(TaskTracker tracker) {
    mapJvmManager = new JvmManagerForType(tracker.getMaxCurrentMapTasks(), 
        true, tracker);
    reduceJvmManager = new JvmManagerForType(tracker.getMaxCurrentReduceTasks(),
        false, tracker);
    this.tracker = tracker;
  }
  
  public void stop() {
    mapJvmManager.stop();
    reduceJvmManager.stop();
  }

  public boolean isJvmKnown(JVMId jvmId) {
    if (jvmId.isMapJVM()) {
      return mapJvmManager.isJvmknown(jvmId);
    } else {
      return reduceJvmManager.isJvmknown(jvmId);
    }
  }

  public void launchJvm(JobID jobId, boolean isMap, JvmEnv env) {
    if (isMap) {
      mapJvmManager.reapJvm(env, jobId, tracker);
    } else {
      reduceJvmManager.reapJvm(env, jobId, tracker);
    }
  }

  public void setRunningTaskForJvm(JVMId jvmId, TaskRunner t) {
    if (jvmId.isMapJVM()) {
      mapJvmManager.setRunningTaskForJvm(jvmId, t);
    } else {
      reduceJvmManager.setRunningTaskForJvm(jvmId, t);
    }
  }

  public void taskFinished(TaskRunner tr) {
    if (tr.getTask().isMapTask()) {
      mapJvmManager.taskFinished(tr);
    } else {
      reduceJvmManager.taskFinished(tr);
    }
  }

  public void taskKilled(TaskRunner tr) {
    if (tr.getTask().isMapTask()) {
      mapJvmManager.taskKilled(tr);
    } else {
      reduceJvmManager.taskKilled(tr);
    }
  }

  public void killJvm(JVMId jvmId) {
    if (jvmId.isMap) {
      mapJvmManager.killJvm(jvmId);
    } else {
      reduceJvmManager.killJvm(jvmId);
    }
  }  

  private static class JvmManagerForType {
    //Mapping from the JVM IDs to running Tasks
    Map <JVMId,TaskRunner> jvmToRunningTask = 
      new HashMap<JVMId, TaskRunner>();
    //Mapping from the tasks to JVM IDs
    Map <TaskRunner,JVMId> runningTaskToJvm = 
      new HashMap<TaskRunner, JVMId>();
    //Mapping from the JVM IDs to Reduce JVM processes
    Map <JVMId, JvmRunner> jvmIdToRunner = 
      new HashMap<JVMId, JvmRunner>();
    int maxJvms;
    boolean isMap;
    
    Random rand = new Random(System.currentTimeMillis());
    TaskTracker tracker;

    public JvmManagerForType(int maxJvms, boolean isMap, TaskTracker tracker) {
      this.maxJvms = maxJvms;
      this.isMap = isMap;
      this.tracker = tracker;
    }

    synchronized public void setRunningTaskForJvm(JVMId jvmId, 
        TaskRunner t) {
      if (t == null) { 
        //signifies the JVM asked for a task and it 
        //was not given anything.
        jvmIdToRunner.get(jvmId).setBusy(false);
        return;
      }
      jvmToRunningTask.put(jvmId, t);
      runningTaskToJvm.put(t,jvmId);
      jvmIdToRunner.get(jvmId).setBusy(true);
    }
    
    synchronized public boolean isJvmknown(JVMId jvmId) {
      return jvmIdToRunner.containsKey(jvmId);
    }

    synchronized public void taskFinished(TaskRunner tr) {
      JVMId jvmId = runningTaskToJvm.remove(tr);
      if (jvmId != null) {
        jvmToRunningTask.remove(jvmId);
        JvmRunner jvmRunner;
        if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null) {
          jvmRunner.taskRan();
        }
      }
    }

    synchronized public void taskKilled(TaskRunner tr) {
      JVMId jvmId = runningTaskToJvm.remove(tr);
      if (jvmId != null) {
        jvmToRunningTask.remove(jvmId);
        killJvm(jvmId);
      }
    }

    synchronized public void killJvm(JVMId jvmId) {
      JvmRunner jvmRunner;
      if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null) {
        jvmRunner.kill();
      }
    }
    
    synchronized public void stop() {
      for (JvmRunner jvm : jvmIdToRunner.values()) {
        jvm.kill();
      }
    }
    
    synchronized private void removeJvm(JVMId jvmId) {
      jvmIdToRunner.remove(jvmId);
    }
    private synchronized void reapJvm( 
        JvmEnv env,
        JobID jobId, TaskTracker tracker) {
      boolean spawnNewJvm = false;
      //Check whether there is a free slot to start a new JVM.
      //,or, Kill a (idle) JVM and launch a new one
      int numJvmsSpawned = jvmIdToRunner.size();

      if (numJvmsSpawned >= maxJvms) {
        //go through the list of JVMs for all jobs.
        //for each JVM see whether it is currently running something and
        //if not, then kill the JVM
        Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = 
          jvmIdToRunner.entrySet().iterator();
        
        while (jvmIter.hasNext()) {
          JvmRunner jvmRunner = jvmIter.next().getValue();
          JobID jId = jvmRunner.jvmId.getJobId();
          //Cases when a JVM is killed: 
          // (1) the JVM under consideration belongs to the same job 
          //     (passed in the argument). In this case, kill only when
          //     the JVM ran all the tasks it was scheduled to run (in terms
          //     of count).
          // (2) the JVM under consideration belongs to a different job and is
          //     currently not busy
          //             
          if ((jId.equals(jobId) && jvmRunner.ranAll()) ||
              (!jId.equals(jobId) && !jvmRunner.isBusy())) {
            jvmIter.remove();
            jvmRunner.kill();
            spawnNewJvm = true;
            break;
          }
        }
      } else {
        spawnNewJvm = true;
      }

      if (spawnNewJvm) {
        spawnNewJvm(jobId, env, tracker);
      } else {
        LOG.info("No new JVM spawned for jobId: " + jobId);
      }
    }

    private void spawnNewJvm(JobID jobId, JvmEnv env, TaskTracker tracker) {
      JvmRunner jvmRunner = new JvmRunner(env,jobId);
      jvmIdToRunner.put(jvmRunner.jvmId, jvmRunner);
      //spawn the JVM in a new thread. Note that there will be very little
      //extra overhead of launching the new thread for a new JVM since
      //most of the cost is involved in launching the process. Moreover,
      //since we are going to be using the JVM for running many tasks,
      //the thread launch cost becomes trivial when amortized over all
      //tasks. Doing it this way also keeps code simple.
      jvmRunner.setDaemon(true);
      jvmRunner.setName("JVM Runner " + jvmRunner.jvmId + " spawned.");
      if (tracker.isTaskMemoryManagerEnabled()) {
        tracker.getTaskMemoryManager().addTask(
            TaskAttemptID.forName(env.conf.get("mapred.task.id")),
            tracker.getMemoryForTask(env.conf));
      }
      LOG.info(jvmRunner.getName());
      jvmRunner.start();
    }
    synchronized private void updateOnJvmExit(JVMId jvmId, 
        int exitCode, boolean killed) {
      removeJvm(jvmId);
      TaskRunner t = jvmToRunningTask.remove(jvmId);

      if (t != null) {
        runningTaskToJvm.remove(t);
        if (!killed && exitCode != 0) {
          t.setExitCode(exitCode);
        }
        t.signalDone();
      }
    }

    private class JvmRunner extends Thread {
      JvmEnv env;
      volatile boolean killed = false;
      volatile int numTasksRan;
      final int numTasksToRun;
      JVMId jvmId;
      volatile boolean busy = true;
      private ShellCommandExecutor shexec; // shell terminal for running the task
      public JvmRunner(JvmEnv env, JobID jobId) {
        this.env = env;
        this.jvmId = new JVMId(jobId, isMap, rand.nextInt());
        this.numTasksToRun = env.conf.getNumTasksToExecutePerJvm();
        LOG.info("In JvmRunner constructed JVM ID: " + jvmId);
      }
      public void run() {
        runChild(env);
      }

      public void runChild(JvmEnv env) {
        try {
          env.vargs.add(Integer.toString(jvmId.getId()));
          List<String> wrappedCommand = 
            TaskLog.captureOutAndError(env.setup, env.vargs, env.stdout, env.stderr,
                env.logSize, env.pidFile);
          shexec = new ShellCommandExecutor(wrappedCommand.toArray(new String[0]), 
              env.workDir, env.env);
          shexec.execute();
        } catch (IOException ioe) {
          // do nothing
          // error and output are appropriately redirected
        } finally { // handle the exit code
          if (shexec == null) {
            return;
          }
          int exitCode = shexec.getExitCode();
          updateOnJvmExit(jvmId, exitCode, killed);
          LOG.info("JVM : " + jvmId +" exited. Number of tasks it ran: " + 
              numTasksRan);
          try {
            //the task jvm cleans up the common workdir for every 
            //task at the beginning of each task in the task JVM.
            //For the last task, we do it here.
            if (env.conf.getNumTasksToExecutePerJvm() != 1) {
              FileUtil.fullyDelete(env.workDir);
            }
          } catch (IOException ie){}
          if (tracker.isTaskMemoryManagerEnabled()) {
          // Remove the associated pid-file, if any
            tracker.getTaskMemoryManager().
               removePidFile(TaskAttemptID.forName(
                   env.conf.get("mapred.task.id")));
          }
        }
      }

      public void kill() {
        if (shexec != null) {
          Process process = shexec.getProcess();
          if (process != null) {
            process.destroy();
          }
        }
        removeJvm(jvmId);
      }
      
      public void taskRan() {
        busy = false;
        numTasksRan++;
      }
      
      public boolean ranAll() {
        return(numTasksRan == numTasksToRun);
      }
      public void setBusy(boolean busy) {
        this.busy = busy;
      }
      public boolean isBusy() {
        return busy;
      }
    }
  }  
  static class JvmEnv { //Helper class
    List<String> vargs;
    List<String> setup;
    File stdout;
    File stderr;
    File workDir;
    String pidFile;
    long logSize;
    JobConf conf;
    Map<String, String> env;

    public JvmEnv(List<String> setup, Vector<String> vargs, File stdout, 
        File stderr, long logSize, File workDir, Map<String,String> env,
        String pidFile, JobConf conf) {
      this.setup = setup;
      this.vargs = vargs;
      this.stdout = stdout;
      this.stderr = stderr;
      this.workDir = workDir;
      this.env = env;
      this.pidFile = pidFile;
      this.conf = conf;
    }
  }
}
