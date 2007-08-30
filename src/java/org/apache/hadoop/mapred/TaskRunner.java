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
import org.apache.hadoop.util.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.util.List;
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

  /** 
   * for cleaning up old map outputs
   */
  protected MapOutputFile mapOutputFile;

  public TaskRunner(Task t, TaskTracker tracker, JobConf conf) {
    this.t = t;
    this.tracker = tracker;
    this.conf = conf;
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }

  public Task getTask() { return t; }
  public TaskTracker getTracker() { return tracker; }

  /** Called to assemble this task's input.  This method is run in the parent
   * process before the child is spawned.  It should not execute user code,
   * only system code. */
  public boolean prepare() throws IOException {
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
    StringBuffer str = new StringBuffer(p[0].toString());
    for (int i = 1; i < p.length; i++){
      str.append(",");
      str.append(p[i].toString());
    }
    return str.toString();
  }
  
  public final void run() {
    try {
      
      //before preparing the job localize 
      //all the archives
      File workDir = new File(t.getJobFile()).getParentFile();
      String taskid = t.getTaskId();
      File jobCacheDir = new File(workDir.getParent(), "work");
      URI[] archives = DistributedCache.getCacheArchives(conf);
      URI[] files = DistributedCache.getCacheFiles(conf);
      if ((archives != null) || (files != null)) {
        if (archives != null) {
          String[] archivesTimestamps = DistributedCache.getArchiveTimestamps(conf);
          Path[] p = new Path[archives.length];
          for (int i = 0; i < archives.length;i++){
            p[i] = DistributedCache.getLocalCache(archives[i], conf, 
                                                  conf.getLocalPath(TaskTracker.getCacheSubdir()), 
                                                  true, Long.parseLong(archivesTimestamps[i]), 
                                                  new Path(workDir.getAbsolutePath()));
          }
          DistributedCache.setLocalArchives(conf, stringifyPathArray(p));
        }
        if ((files != null)) {
          String[] fileTimestamps = DistributedCache.getFileTimestamps(conf);
          Path[] p = new Path[files.length];
          for (int i = 0; i < files.length;i++){
            p[i] = DistributedCache.getLocalCache(files[i], conf, 
                                                  conf.getLocalPath(TaskTracker.getCacheSubdir()), 
                                                  false, Long.parseLong(fileTimestamps[i]), 
                                                  new Path(workDir.getAbsolutePath()));
          }
          DistributedCache.setLocalFiles(conf, stringifyPathArray(p));
        }
        Path localTaskFile = new Path(t.getJobFile());
        FileSystem localFs = FileSystem.getLocal(conf);
        localFs.delete(localTaskFile);
        OutputStream out = localFs.create(localTaskFile);
        try {
          conf.write(out);
        } finally {
          out.close();
        }
      }
    
      // create symlinks for all the files in job cache dir in current
      // workingdir for streaming
      try{
        DistributedCache.createAllSymlink(conf, jobCacheDir, 
                                          workDir);
      } catch(IOException ie){
        // Do not exit even if symlinks have not been created.
        LOG.warn(StringUtils.stringifyException(ie));
      }
      
      if (!prepare()) {
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
        File[] libs = new File(jobCacheDir, "lib").listFiles();
        if (libs != null) {
          for (int i = 0; i < libs.length; i++) {
            classPath.append(sep);            // add libs from jar to classpath
            classPath.append(libs[i]);
          }
        }
        classPath.append(sep);
        classPath.append(new File(jobCacheDir, "classes"));
        classPath.append(sep);
        classPath.append(jobCacheDir);
       
      }

      // include the user specified classpath
  		
      //archive paths
      Path[] archiveClasspaths = DistributedCache.getArchiveClassPaths(conf);
      if (archiveClasspaths != null && archives != null) {
        Path[] localArchives = DistributedCache
          .getLocalCacheArchives(conf);
        if (localArchives != null){
          for (int i=0;i<archives.length;i++){
            for(int j=0;j<archiveClasspaths.length;j++){
              if (archives[i].getPath().equals(
                                               archiveClasspaths[j].toString())){
                classPath.append(sep);
                classPath.append(localArchives[i]
                                 .toString());
              }
            }
          }
        }
      }
      //file paths
      Path[] fileClasspaths = DistributedCache.getFileClassPaths(conf);
      if (fileClasspaths!=null && files != null) {
        Path[] localFiles = DistributedCache
          .getLocalCacheFiles(conf);
        if (localFiles != null) {
          for (int i = 0; i < files.length; i++) {
            for (int j = 0; j < fileClasspaths.length; j++) {
              if (files[i].getPath().equals(
                                            fileClasspaths[j].toString())) {
                classPath.append(sep);
                classPath.append(localFiles[i].toString());
              }
            }
          }
        }
      }

      classPath.append(sep);
      classPath.append(workDir);
      //  Build exec child jmv args.
      Vector<String> vargs = new Vector<String>(8);
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
        //     </value>
        //
        String javaOpts = handleDeprecatedHeapSize(
                                                   conf.get("mapred.child.java.opts", "-Xmx200m"),
                                                   conf.get("mapred.child.heap.size"));
        javaOpts = replaceAll(javaOpts, "@taskid@", taskid);
        String [] javaOptsSplit = javaOpts.split(" ");
        //Add java.library.path; necessary for native-hadoop libraries
        String libraryPath = System.getProperty("java.library.path");
        if (libraryPath != null) {
          boolean hasLibrary = false;
          for(int i=0; i<javaOptsSplit.length ;i++) { 
            if(javaOptsSplit[i].startsWith("-Djava.library.path=")) {
              javaOptsSplit[i] += sep + libraryPath;
              hasLibrary = true;
              break;
            }
          }
          if(!hasLibrary)
            vargs.add("-Djava.library.path=" + libraryPath);
        }
        for (int i = 0; i < javaOptsSplit.length; i++) {
          vargs.add(javaOptsSplit[i]);
        }
        
        // Add classpath.
        vargs.add("-classpath");
        vargs.add(classPath.toString());

        // Setup the log4j prop
        long logSize = TaskLog.getTaskLogLength(conf);
        vargs.add("-Dhadoop.log.dir=" + 
                  new File(System.getProperty("hadoop.log.dir")
                           ).getAbsolutePath());
        vargs.add("-Dhadoop.root.logger=INFO,TLA");
        vargs.add("-Dhadoop.tasklog.taskid=" + taskid);
        vargs.add("-Dhadoop.tasklog.totalLogFileSize=" + logSize);

        // Add main class and its arguments 
        vargs.add(TaskTracker.Child.class.getName());  // main of Child
        // pass umbilical address
        InetSocketAddress address = tracker.getTaskTrackerReportAddress();
        vargs.add(address.getAddress().getHostAddress()); 
        vargs.add(Integer.toString(address.getPort())); 
        vargs.add(taskid);                      // pass task identifier

        // Run java
        File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
        File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);
        stdout.getParentFile().mkdirs();

        List<String> wrappedCommand = 
          TaskLog.captureOutAndError(vargs, stdout, stderr, logSize);
        runChild(wrappedCommand, workDir, taskid);
                 
    } catch (FSError e) {
      LOG.fatal("FSError", e);
      try {
        tracker.fsError(t.getTaskId(), e.getMessage());
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
   * Append the contents of the input stream to the output file. Both streams 
   * are closed upon exit.
   * @param in the stream to read
   * @param outName the filename to append the data to
   * @throws IOException if something goes wrong
   */
  private void copyStream(InputStream in, File outName) throws IOException {
    try {
      OutputStream out = new FileOutputStream(outName, true);
      try {
        byte[] buffer = new byte[1024];
        int len = in.read(buffer);
        while (len > 0) {
          out.write(buffer, 0, len);
          len = in.read(buffer);
        }
      } finally {
        out.close();
      }
    } finally {
      in.close();
    }
  }

  /**
   * Run the child process
   */
  private void runChild(List<String> args, File dir,
                        String taskid) throws IOException {
    ProcessBuilder builder = new ProcessBuilder(args);
    builder.directory(dir);
    process = builder.start();
    
    try {
      int exit_code = process.waitFor();
     
      if (!killed && exit_code != 0) {
        if (exit_code == 65) {
          tracker.getTaskTrackerMetrics().taskFailedPing();
        }
        copyStream(process.getInputStream(), 
                   TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT));
        copyStream(process.getErrorStream(), 
                   TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR));
        throw new IOException("Task process exit with nonzero status of " +
                              exit_code + ".");
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

}
