/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.logging.*;

/** Base class that runs a task in a separate process.  Tasks are run in a
 * separate process in order to isolate the map/reduce system code from bugs in
 * user supplied map and reduce functions.
 */
abstract class TaskRunner extends Thread {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.TaskRunner");

  boolean killed = false;
  private Process process;
  private Task t;
  private TaskTracker tracker;

  protected Configuration conf;

  public TaskRunner(Task t, TaskTracker tracker, Configuration conf) {
    this.t = t;
    this.tracker = tracker;
    this.conf = conf;
  }

  public Task getTask() { return t; }
  public TaskTracker getTracker() { return tracker; }

  /** Called to assemble this task's input.  This method is run in the parent
   * process before the child is spawned.  It should not execute user code,
   * only system code. */
  public boolean prepare() throws IOException {return true;}

  /** Called when this task's output is no longer needed.
  * This method is run in the parent process after the child exits.  It should
  * not execute user code, only system code.
  */
  public void close() throws IOException {}

  public final void run() {
    try {

      if (! prepare()) {
        return;
      }

      String sep = System.getProperty("path.separator");
      File workDir = new File(new File(t.getJobFile()).getParent(), "work");
      workDir.mkdirs();
               
      StringBuffer classPath = new StringBuffer();
      // start with same classpath as parent process
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(sep);

      JobConf job = new JobConf(t.getJobFile());
      String jar = job.getJar();
      if (jar != null) {                      // if jar exists, it into workDir
        runChild(new String[] { "unzip", "-o", "-q", jar}, workDir);
        File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
          for (int i = 0; i < libs.length; i++) {
            classPath.append(sep);            // add libs from jar to classpath
            classPath.append(libs[i]);
          }
        }
        classPath.append(sep);
        classPath.append(new File(workDir, "classes"));
        classPath.append(sep);
        classPath.append(workDir);
      }

      File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");
        
      // run java
      runChild(new String[] {
        jvm.toString(),
        //"-Xrunhprof:cpu=samples,file="+t.getTaskId()+".prof",
        "-Xmx"+job.get("mapred.child.heap.size", "200m"),
        "-cp", classPath.toString(),
        TaskTracker.Child.class.getName(),        // main is Child
        tracker.taskReportPort+"",                // pass umbilical port
        t.getTaskId()                             // pass task identifier
      }, workDir);

    } catch (FSError e) {
      LOG.log(Level.SEVERE, "FSError", e);
      try {
        tracker.fsError(e.getMessage());
      } catch (IOException ie) {
        LOG.log(Level.SEVERE, t.getTaskId()+" reporting FSError", ie);
      }
    } catch (Throwable throwable) {
      LOG.log(Level.WARNING, t.getTaskId()+" Child Error", throwable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      throwable.printStackTrace(new PrintStream(baos));
      try {
        tracker.reportDiagnosticInfo(t.getTaskId(), baos.toString());
      } catch (IOException e) {
        LOG.log(Level.WARNING, t.getTaskId()+" Reporting Diagnostics", e);
      }
    } finally {
      tracker.reportTaskFinished(t.getTaskId());
    }
  }

  /**
   * Run the child process
   */
  private void runChild(String[] args, File dir) throws IOException {
    this.process = Runtime.getRuntime().exec(args, null, dir);
    try {
      StringBuffer errorBuf = new StringBuffer();
      new Thread() {
        public void run() {
          logStream(process.getErrorStream());    // copy log output
        }
      }.start();
        
      logStream(process.getInputStream());        // normally empty
      
      if (this.process.waitFor() != 0) {
        throw new IOException("Task process exit with nonzero status.");
      }
      
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      kill();
    }
  }

  /**
   * Kill the child process
   */
  public void kill() {
      if (process != null) {
          process.destroy();
      }
      killed = true;
  }

  /**
   */
  private void logStream(InputStream output) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(output));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info(t.getTaskId()+" "+line);
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, t.getTaskId()+" Error reading child output", e);
    } finally {
      try {
        output.close();
      } catch (IOException e) {
        LOG.log(Level.WARNING, t.getTaskId()+" Error closing child output", e);
      }
    }
  }
}
