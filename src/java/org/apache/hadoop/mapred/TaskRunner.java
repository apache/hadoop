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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.*;
import java.io.*;
import java.util.Vector;
import java.net.URI;

/** Base class that runs a task in a separate process.  Tasks are run in a
 * separate process in order to isolate the map/reduce system code from bugs in
 * user supplied map and reduce functions.
 */
abstract class TaskRunner extends Thread {
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.TaskRunner");

  volatile boolean killed = false;
  private Process process;
  private Task t;
  private TaskTracker tracker;

  protected JobConf conf;

  private TaskLog.Writer taskLogWriter;
  
  public TaskRunner(Task t, TaskTracker tracker, JobConf conf) {
    this.t = t;
    this.tracker = tracker;
    this.conf = conf;
    this.taskLogWriter = new TaskLog.Writer(conf, t.getTaskId());
  }

  public Task getTask() { return t; }
  public TaskTracker getTracker() { return tracker; }

  /** Called to assemble this task's input.  This method is run in the parent
   * process before the child is spawned.  It should not execute user code,
   * only system code. */
  public boolean prepare() throws IOException {
    taskLogWriter.init();                   // initialize the child task's log
    return true;
  }

  /** Called when this task's output is no longer needed.
  * This method is run in the parent process after the child exits.  It should
  * not execute user code, only system code.
  */
  public void close() throws IOException {}

  private String stringifyPathArray(Path[] p){
	  if (p == null){
      return null;
    }
    String str = p[0].toString();
    for (int i = 1; i < p.length; i++){
      str = str + "," + p[i].toString();
    }
    return str;
  }
  
  public final void run() {
    try {
      
      //before preparing the job localize 
      //all the archives
      File workDir = new File(new File(t.getJobFile()).getParentFile().getParent(), "work");
      URI[] archives = DistributedCache.getCacheArchives(conf);
      URI[] files = DistributedCache.getCacheFiles(conf);
      if ((archives != null) || (files != null)) {
        if (archives != null) {
          String[] md5 = DistributedCache.getArchiveMd5(conf);
          Path[] p = new Path[archives.length];
          for (int i = 0; i < archives.length;i++){
            p[i] = DistributedCache.getLocalCache(archives[i], conf, 
                conf.getLocalPath(TaskTracker.getCacheSubdir()), true, md5[i], new Path(workDir.getAbsolutePath()));
          }
          DistributedCache.setLocalArchives(conf, stringifyPathArray(p));
        }
        if ((files != null)) {
          String[] md5 = DistributedCache.getFileMd5(conf);
          Path[] p = new Path[files.length];
          for (int i = 0; i < files.length;i++){
           p[i] = DistributedCache.getLocalCache(files[i], conf, conf.getLocalPath(TaskTracker
              .getCacheSubdir()), false, md5[i], new Path(workDir.getAbsolutePath()));
          }
          DistributedCache.setLocalFiles(conf, stringifyPathArray(p));
        }
        
        // sets the paths to local archives and paths
        Path localTaskFile = new Path(t.getJobFile());
        FileSystem localFs = FileSystem.getNamed("local", conf);
        localFs.delete(localTaskFile);
        OutputStream out = localFs.create(localTaskFile);
        try {
          conf.write(out);
        } finally {
          out.close();
        }
      }
      
      if (! prepare()) {
        return;
      }

      String sep = System.getProperty("path.separator");
      StringBuffer classPath = new StringBuffer();
      // start with same classpath as parent process
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(sep);
      if (!workDir.mkdirs()) {
        if (!workDir.isDirectory()) {
          LOG.fatal("Mkdirs failed to create " + workDir.toString());
        }
      }
	  
      String jar = conf.getJar();
      if (jar != null) {       
    	  // if jar exists, it into workDir
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

      //  Build exec child jmv args.
      Vector vargs = new Vector(8);
      File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");

      vargs.add(jvm.toString());

      // Add child java ops.  Also, mapred.child.heap.size has been superceded
      // by // mapred.child.java.opts.  Manage case where both are present
      // letting the mapred.child.heap.size win over any setting of heap size in
      // mapred.child.java.opts (Emit a warning that heap.size is deprecated).
      //
      // The following symbols if present in mapred.child.java.opts value are
      // replaced:
      // + @taskid@ is interpolated with value of TaskID.
      // + Replaces @port@ with mapred.task.tracker.report.port + 1.
      // Other occurrences of @ will not be altered.
      //
      // Example with multiple arguments and substitutions, showing
      // jvm GC logging, and start of a passwordless JVM JMX agent so can
      // connect with jconsole and the likes to watch child memory, threads
      // and get thread dumps.
      //
      //     <name>mapred.child.optional.jvm.args</name>
      //     <value>-verbose:gc -Xloggc:/tmp/@taskid@.gc \
      //     -Dcom.sun.management.jmxremote.authenticate=false \
      //     -Dcom.sun.management.jmxremote.ssl=false \
      //     -Dcom.sun.management.jmxremote.port=@port@
      //     </value>
      //
      String javaOpts = handleDeprecatedHeapSize(
          conf.get("mapred.child.java.opts", "-Xmx200m"),
          conf.get("mapred.child.heap.size"));
      javaOpts = replaceAll(javaOpts, "@taskid@", t.getTaskId());
      int port = conf.getInt("mapred.task.tracker.report.port", 50050) + 1;
      javaOpts = replaceAll(javaOpts, "@port@", Integer.toString(port));
      String [] javaOptsSplit = javaOpts.split(" ");
      for (int i = 0; i < javaOptsSplit.length; i++) {
         vargs.add(javaOptsSplit[i]);
      }

      // Add classpath.
      vargs.add("-classpath");
      vargs.add(classPath.toString());
      // Add main class and its arguments 
      vargs.add(TaskTracker.Child.class.getName());  // main of Child
      vargs.add(tracker.taskReportPort + "");        // pass umbilical port
      vargs.add(t.getTaskId());                      // pass task identifier
      // Run java
      runChild((String[])vargs.toArray(new String[0]), workDir);
    } catch (FSError e) {
      LOG.fatal("FSError", e);
      try {
        tracker.fsError(e.getMessage());
      } catch (IOException ie) {
        LOG.fatal(t.getTaskId()+" reporting FSError", ie);
      }
    } catch (Throwable throwable) {
      LOG.warn(t.getTaskId()+" Child Error", throwable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      throwable.printStackTrace(new PrintStream(baos));
      try {
        tracker.reportDiagnosticInfo(t.getTaskId(), baos.toString());
      } catch (IOException e) {
        LOG.warn(t.getTaskId()+" Reporting Diagnostics", e);
      }
    } finally {
      try{
        URI[] archives = DistributedCache.getCacheArchives(conf);
        URI[] files = DistributedCache.getCacheFiles(conf);
        if (archives != null){
          for (int i = 0; i < archives.length; i++){
            DistributedCache.releaseCache(archives[i], conf);
          }
        }
        if (files != null){
          for(int i = 0; i < files.length; i++){
            DistributedCache.releaseCache(files[i], conf);
          }
        }
      }catch(IOException ie){
        LOG.warn("Error releasing caches : Cache files might not have been cleaned up");
      }
      tracker.reportTaskFinished(t.getTaskId());
    }
  }

  
  /**
   * Handle deprecated mapred.child.heap.size.
   * If present, interpolate into mapred.child.java.opts value with
   * warning.
   * @param javaOpts Value of mapred.child.java.opts property.
   * @param heapSize Value of mapred.child.heap.size property.
   * @return A <code>javaOpts</code> with <code>heapSize</code>
   * interpolated if present.
   */
  private String handleDeprecatedHeapSize(String javaOpts,
          final String heapSize) {
    if (heapSize == null || heapSize.length() <= 0) {
        return javaOpts;
    }
    final String MX = "-Xmx";
    int index = javaOpts.indexOf(MX);
    if (index < 0) {
        javaOpts = javaOpts + " " + MX + heapSize;
    } else {
        int end = javaOpts.indexOf(" ", index + MX.length());
        javaOpts = javaOpts.substring(0, index + MX.length()) +
            heapSize + ((end < 0)? "": javaOpts.substring(end));
    }
    LOG.warn("mapred.child.heap.size is deprecated. Use " +
        "mapred.child.java.opt instead. Meantime, mapred.child.heap.size " +
        "is interpolated into mapred.child.java.opt: " + javaOpts);
    return javaOpts;
  }

  /**
   * Replace <code>toFind</code> with <code>replacement</code>.
   * When hadoop moves to JDK1.5, replace this method with
   * String#replace (Of is commons-lang available, replace with
   * StringUtils#replace). 
   * @param text String to do replacements in.
   * @param toFind String to find.
   * @param replacement String to replace <code>toFind</code> with.
   * @return A String with all instances of <code>toFind</code>
   * replaced by <code>replacement</code> (The original
   * <code>text</code> is returned if <code>toFind</code> is not
   * found in <code>text<code>).
   */
  private static String replaceAll(String text, final String toFind,
      final String replacement) {
    if (text ==  null || toFind ==  null || replacement ==  null) {
      throw new IllegalArgumentException("Text " + text + " or toFind " +
        toFind + " or replacement " + replacement + " are null.");
    }
    int offset = 0;
    for (int index = text.indexOf(toFind); index >= 0;
          index = text.indexOf(toFind, offset)) {
      offset = index + toFind.length();
      text = text.substring(0, index) + replacement +
          text.substring(offset);
        
    }
    return text;
  }

  /**
   * Run the child process
   */
  private void runChild(String[] args, File dir) throws IOException {
    this.process = Runtime.getRuntime().exec(args, null, dir);
    try {
      new Thread() {
        public void run() {
          logStream(process.getErrorStream());    // copy log output
        }
      }.start();
        
      logStream(process.getInputStream());		  // normally empty
      
      int exit_code = process.waitFor();
     
      if (!killed && exit_code != 0) {
        throw new IOException("Task process exit with nonzero status of " +
                              exit_code + ".");
      }
      
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      kill();
      taskLogWriter.close();
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
      byte[] buf = new byte[512];
      int n = 0;
      while ((n = output.read(buf, 0, buf.length)) != -1) {
        // Write out to the task's log
        taskLogWriter.write(buf, 0, n);
      }
    } catch (IOException e) {
      LOG.warn(t.getTaskId()+" Error reading child output", e);
    } finally {
      try {
        output.close();
      } catch (IOException e) {
        LOG.warn(t.getTaskId()+" Error closing child output", e);
      }
    }
  }
  
}